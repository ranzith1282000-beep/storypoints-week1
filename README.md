#!/usr/bin/env python3
import os
import requests
import pandas as pd
import json
from datetime import date
from google.cloud import storage
from io import StringIO

# --- Configuration ---
BUCKET = "storypoints-data-bucket_ranjith"  # ✅ your GCS bucket
API_KEY = "8f9bea72a6dd1ffbb2d00bbb"        # ✅ your working API key
today = str(date.today())

# --- Folder setup (permission-safe paths) ---
RAW_API_PATH = os.path.expanduser("~/storypoints_week1/data/raw/api_currency")
CLEANED_PATH = os.path.expanduser("~/storypoints_week1/data/cleaned")
os.makedirs(RAW_API_PATH, exist_ok=True)
os.makedirs(CLEANED_PATH, exist_ok=True)

print("📥 Fetching exchange rate data...")

# --- 1. EXTRACT (API data) ---
url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
response = requests.get(url)

if response.status_code == 200:
    with open(f"{RAW_API_PATH}/exchange_rates.json", "w") as f:
        f.write(response.text)
    print("✅ Exchange rate data saved successfully.")
else:
    print("❌ API request failed:", response.status_code)
    exit()

# --- 2. TRANSFORM (CSV data) ---
print("⚙️ Cleaning and transforming CSV data...")

client = storage.Client()
bucket = client.bucket(BUCKET)

# Download raw CSVs from GCS
click_blob = bucket.blob("clickstream.csv")
tran_blob = bucket.blob("transactions.csv")

click_data = click_blob.download_as_text()
tran_data = tran_blob.download_as_text()

clickstream = pd.read_csv(StringIO(click_data))
transactions = pd.read_csv(StringIO(tran_data))

# Load exchange rate JSON
with open(f"{RAW_API_PATH}/exchange_rates.json") as f:
    rates = json.load(f)["conversion_rates"]

# Clean column names
clickstream.columns = clickstream.columns.str.lower().str.replace(" ", "_")
transactions.columns = transactions.columns.str.lower().str.replace(" ", "_")

# Handle missing values and duplicates
clickstream.fillna("Unknown", inplace=True)
transactions.fillna({"customer_id": "Unknown", "amount": 0}, inplace=True)
clickstream.drop_duplicates(inplace=True)
transactions.drop_duplicates(inplace=True)

# Convert timestamps
if "timestamp" in clickstream.columns:
    clickstream["timestamp"] = pd.to_datetime(clickstream["timestamp"], errors="coerce", utc=True)
if "timestamp" in transactions.columns:
    transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], errors="coerce", utc=True)

# Convert amounts to USD (if currency column exists)
if "currency" in transactions.columns:
    transactions["amount_in_usd"] = transactions["amount"] / transactions["currency"].map(rates)

# Save cleaned files locally
clickstream.to_csv(f"{CLEANED_PATH}/clickstream_cleaned.csv", index=False)
transactions.to_csv(f"{CLEANED_PATH}/transactions_cleaned.csv", index=False)
print("✅ Cleaned data saved locally.")

# --- 3. LOAD (upload cleaned data back to GCS) ---
print("☁️ Uploading cleaned files to GCS...")

datasets = {
    "clickstream_cleaned.csv": f"data/cleaned/clickstream/ingest_date={today}/clickstream_cleaned.csv",
    "transactions_cleaned.csv": f"data/cleaned/transactions/ingest_date={today}/transactions_cleaned.csv"
}

for file, dest in datasets.items():
    blob = bucket.blob(dest)
    blob.upload_from_filename(f"{CLEANED_PATH}/{file}")
    print(f"✅ Uploaded {file} → gs://{BUCKET}/{dest}")

print("🎉 ETL pipeline completed successfully!")
