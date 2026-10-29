"""Clickstream and transactions ETL pipeline on Google Cloud Storage.

Extract:   raw clickstream and transactions CSVs (from a GCS bucket or local
           files) plus live USD exchange rates from ExchangeRate-API.
Transform: standardise column names, parse timestamps to UTC, run data
           quality checks, remove duplicates and add ``amount_in_usd``.
Load:      write cleaned files to date-partitioned paths
           (``data/cleaned/<dataset>/ingest_date=YYYY-MM-DD/``) in GCS,
           or to a local folder.

Run locally with the bundled sample data (no cloud account or API key):

    python src/etl_pipeline.py --local

Run against GCS (needs Google Cloud credentials and an API key):

    export GCS_BUCKET=your-bucket
    export EXCHANGE_RATE_API_KEY=your-key
    python src/etl_pipeline.py
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import date
from io import StringIO
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = ROOT / "data" / "sample"
SAMPLE_RATES = SAMPLE_DIR / "exchange_rates_2025-11-10.json"
LOCAL_OUT = ROOT / "output"
RATES_URL = "https://v6.exchangerate-api.com/v6/{key}/latest/USD"

CLICKSTREAM_COLUMNS = ["user_id", "session_id", "page_url", "click_time", "device", "location"]
TRANSACTION_COLUMNS = ["txn_id", "user_id", "amount", "currency", "txn_time"]


@dataclass
class QualityReport:
    """What the checks found in one dataset."""
    dataset: str
    rows_in: int = 0
    rows_out: int = 0
    nulls: dict = field(default_factory=dict)
    duplicates_removed: int = 0
    unparseable_timestamps: int = 0
    notes: list = field(default_factory=list)

    def summary(self) -> str:
        null_total = sum(self.nulls.values())
        lines = [f"{self.dataset}: {self.rows_in:,} rows in, {self.rows_out:,} rows out",
                 f"  nulls: {null_total}  duplicates removed: {self.duplicates_removed}"
                 f"  unparseable timestamps: {self.unparseable_timestamps}"]
        lines += [f"  note: {n}" for n in self.notes]
        return "\n".join(lines)


# ---------- Extract ----------

def fetch_rates(api_key: str) -> dict:
    """Return USD-based conversion rates, e.g. {"GBP": 0.76, ...}."""
    import requests
    resp = requests.get(RATES_URL.format(key=api_key), timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("result") != "success":
        raise RuntimeError(f"ExchangeRate-API error: {body.get('error-type', 'unknown')}")
    return body["conversion_rates"]


def load_rates_file(path: Path) -> dict:
    return json.loads(Path(path).read_text())["conversion_rates"]


def read_gcs_csv(bucket, name: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(bucket.blob(name).download_as_text()))


# ---------- Transform ----------

def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def _require(df: pd.DataFrame, columns: list, dataset: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset} is missing columns: {missing}")


def clean_clickstream(raw: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
    df = standardise_columns(raw)
    _require(df, CLICKSTREAM_COLUMNS, "clickstream")
    rep = QualityReport("clickstream", rows_in=len(df), nulls=df.isna().sum().to_dict())

    for col in ["page_url", "device", "location"]:
        df[col] = df[col].fillna("unknown")
    df["device"] = df["device"].str.lower()

    df["click_time"] = pd.to_datetime(df["click_time"], errors="coerce", utc=True)
    rep.unparseable_timestamps = int(df["click_time"].isna().sum())
    df = df.dropna(subset=["click_time"])

    before = len(df)
    df = df.drop_duplicates()
    rep.duplicates_removed = before - len(df)
    rep.rows_out = len(df)
    return df.reset_index(drop=True), rep


def clean_transactions(raw: pd.DataFrame, rates: dict) -> tuple[pd.DataFrame, QualityReport]:
    df = standardise_columns(raw)
    _require(df, TRANSACTION_COLUMNS, "transactions")
    rep = QualityReport("transactions", rows_in=len(df), nulls=df.isna().sum().to_dict())

    df["currency"] = df["currency"].str.upper().str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["txn_id", "amount", "currency"])

    df["txn_time"] = pd.to_datetime(df["txn_time"], errors="coerce", utc=True)
    rep.unparseable_timestamps = int(df["txn_time"].isna().sum())
    df = df.dropna(subset=["txn_time"])

    # A transaction ID should appear once; keep the first occurrence.
    before = len(df)
    df = df.drop_duplicates(subset="txn_id", keep="first")
    rep.duplicates_removed = before - len(df)

    # Rates are "units of currency per 1 USD", so divide to convert to USD.
    rate = df["currency"].map(rates)
    unknown = df.loc[rate.isna(), "currency"].unique().tolist()
    if unknown:
        rep.notes.append(f"no exchange rate for {unknown}; amount_in_usd left empty")
    df["amount_in_usd"] = (df["amount"] / rate).round(2)

    negative = int((df["amount"] < 0).sum())
    if negative:
        rep.notes.append(f"{negative} negative amounts (refunds?) kept as-is")
    rep.rows_out = len(df)
    return df.reset_index(drop=True), rep


# ---------- Load ----------

def partition_path(dataset: str, ingest_date: str) -> str:
    return f"data/cleaned/{dataset}/ingest_date={ingest_date}/{dataset}_cleaned.csv"


def write_local(df: pd.DataFrame, dataset: str, ingest_date: str, out_dir: Path | None = None) -> Path:
    path = Path(out_dir or LOCAL_OUT) / partition_path(dataset, ingest_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def write_gcs(df: pd.DataFrame, bucket, dataset: str, ingest_date: str) -> str:
    dest = partition_path(dataset, ingest_date)
    bucket.blob(dest).upload_from_string(df.to_csv(index=False), content_type="text/csv")
    return f"gs://{bucket.name}/{dest}"


# ---------- Run ----------

def run(local: bool, rates_file: Path | None, ingest_date: str) -> list[QualityReport]:
    if rates_file:
        rates = load_rates_file(rates_file)
    elif local and not os.getenv("EXCHANGE_RATE_API_KEY"):
        rates = load_rates_file(SAMPLE_RATES)
    else:
        key = os.environ.get("EXCHANGE_RATE_API_KEY")
        if not key:
            raise SystemExit("Set EXCHANGE_RATE_API_KEY, or pass --rates-file.")
        rates = fetch_rates(key)

    if local:
        clicks_raw = pd.read_csv(SAMPLE_DIR / "clickstream.csv")
        txns_raw = pd.read_csv(SAMPLE_DIR / "transactions.csv")
    else:
        from google.cloud import storage
        bucket_name = os.environ.get("GCS_BUCKET")
        if not bucket_name:
            raise SystemExit("Set GCS_BUCKET to the bucket that holds the raw CSVs.")
        bucket = storage.Client().bucket(bucket_name)
        clicks_raw = read_gcs_csv(bucket, "clickstream.csv")
        txns_raw = read_gcs_csv(bucket, "transactions.csv")

    clicks, click_rep = clean_clickstream(clicks_raw)
    txns, txn_rep = clean_transactions(txns_raw, rates)

    for df, name in [(clicks, "clickstream"), (txns, "transactions")]:
        where = write_local(df, name, ingest_date) if local else write_gcs(df, bucket, name, ingest_date)
        print(f"Wrote {name} -> {where}")
    for rep in (click_rep, txn_rep):
        print(rep.summary())
    return [click_rep, txn_rep]


def main() -> None:
    parser = argparse.ArgumentParser(description="Clickstream and transactions ETL on GCS")
    parser.add_argument("--local", action="store_true", help="use bundled sample data and write to ./output")
    parser.add_argument("--rates-file", type=Path, help="use saved exchange rates instead of calling the API")
    parser.add_argument("--ingest-date", default=str(date.today()), help="partition date (default: today)")
    args = parser.parse_args()
    run(args.local, args.rates_file, args.ingest_date)


if __name__ == "__main__":
    main()
