# fetch_jolts.py
import io
from pathlib import Path
import pandas as pd
import requests
import sys

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=JTSJOL"
DATA_DIR = Path("data")
RAW_PATH = DATA_DIR / "jtsjol_raw.csv"
PROC_PATH = DATA_DIR / "jtsjol_processed.csv"

def fetch_csv(url: str) -> str:
    print("🔄 Fetching CSV from FRED...")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; GitHubActionsBot/1.0)"
    }
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"❌ Error fetching CSV: {e}")
        sys.exit(1)

def to_processed(csv_text: str) -> pd.DataFrame:
    print("🔍 Parsing CSV and validating structure...")

    try:
        df = pd.read_csv(io.StringIO(csv_text))
    except Exception as e:
        print("❌ Failed to parse CSV:", e)
        print("🔎 CSV Preview:\n", csv_text[:500])
        raise

    if "DATE" not in df.columns or "JTSJOL" not in df.columns:
        print("❌ Missing expected 'DATE' and 'JTSJOL' columns.")
        print("CSV Columns:", df.columns.tolist())
        print("🔎 CSV Preview:\n", csv_text[:500])
        raise ValueError("Expected 'DATE' and 'JTSJOL' columns not found.")

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["value"] = pd.to_numeric(df["JTSJOL"], errors="coerce")

    df = df[df["DATE"] >= pd.Timestamp("2020-01-01")].dropna(subset=["value"]).copy()

    if df.empty:
        raise ValueError("No data remaining after 2020-01-01 filter.")

    base = df.iloc[0]["value"]
    df["month_end"] = df["DATE"].dt.strftime("%Y-%m-%d")
    df["JOLTS"] = (df["value"] / base * 100).round(2)
    df["type"] = "JOLTS"

    return df[["month_end", "value", "JOLTS", "type"]]

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    csv_text = fetch_csv(FRED_CSV_URL)

    RAW_PATH.write_text(csv_text, encoding="utf-8")
    print(f"✅ Wrote raw CSV to: {RAW_PATH}")

    try:
        processed = to_processed(csv_text)
        processed.to_csv(PROC_PATH, index=False)
        print(f"✅ Wrote processed CSV to: {PROC_PATH}")
        print(f"📈 Rows in processed CSV: {len(processed)}")
    except Exception as e:
        print("❌ Failed to process CSV:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
