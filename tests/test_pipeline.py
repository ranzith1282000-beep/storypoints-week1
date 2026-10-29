"""Tests for the cleaning logic. They run offline: no API key or cloud account."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import etl_pipeline as etl  # noqa: E402

RATES = {"USD": 1.0, "GBP": 0.8, "EUR": 0.9}


def test_transactions_converted_to_usd():
    raw = pd.DataFrame({"txn_id": ["a", "b"], "user_id": [1, 2], "amount": [80.0, 50.0],
                        "currency": ["GBP", "usd"], "txn_time": ["2025-09-02T10:00:00Z"] * 2})
    out, rep = etl.clean_transactions(raw, RATES)
    assert out["amount_in_usd"].tolist() == [100.0, 50.0]
    assert rep.rows_out == 2


def test_duplicate_transaction_ids_removed():
    raw = pd.DataFrame({"txn_id": ["a", "a", "b"], "user_id": [1, 1, 2], "amount": [10.0, 10.0, 5.0],
                        "currency": ["USD"] * 3, "txn_time": ["2025-09-02T10:00:00Z"] * 3})
    out, rep = etl.clean_transactions(raw, RATES)
    assert rep.duplicates_removed == 1 and len(out) == 2


def test_unknown_currency_is_flagged_not_dropped():
    raw = pd.DataFrame({"txn_id": ["a"], "user_id": [1], "amount": [10.0],
                        "currency": ["XYZ"], "txn_time": ["2025-09-02T10:00:00Z"]})
    out, rep = etl.clean_transactions(raw, RATES)
    assert len(out) == 1 and pd.isna(out.loc[0, "amount_in_usd"])
    assert any("XYZ" in n for n in rep.notes)


def test_bad_timestamps_counted_and_dropped():
    raw = pd.DataFrame({"user_id": [1, 2], "session_id": ["s1", "s2"], "page_url": ["/", "/cart"],
                        "click_time": ["2025-09-02T10:00:00Z", "not a date"],
                        "device": ["Mobile", None], "location": ["GB", "US"]})
    out, rep = etl.clean_clickstream(raw)
    assert rep.unparseable_timestamps == 1 and len(out) == 1
    assert out.loc[0, "device"] == "mobile"


def test_missing_column_raises():
    with pytest.raises(ValueError, match="missing columns"):
        etl.clean_clickstream(pd.DataFrame({"user_id": [1]}))


def test_partition_path():
    assert etl.partition_path("clickstream", "2025-11-10") == \
        "data/cleaned/clickstream/ingest_date=2025-11-10/clickstream_cleaned.csv"


def test_sample_run_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, "LOCAL_OUT", tmp_path)
    monkeypatch.delenv("EXCHANGE_RATE_API_KEY", raising=False)
    reports = etl.run(local=True, rates_file=None, ingest_date="2025-11-10")
    assert {r.dataset for r in reports} == {"clickstream", "transactions"}
    txns = pd.read_csv(tmp_path / etl.partition_path("transactions", "2025-11-10"))
    assert txns["amount_in_usd"].notna().all()
