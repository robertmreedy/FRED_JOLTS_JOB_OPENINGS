# fetch_jolts.py
import io
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=JTSJOL"
DATA_DIR = Path("data")
RAW_PATH = DATA_DIR / "jtsjol_raw.csv"
PROC_PATH = DATA_DIR / "jtsjol_processed.csv"

def fetch_csv(url: str) -> str:
    # Server-side fetch; CORS is not an issue in GitHub Actions.
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.text

def to_processed(csv_text: str) -> pd.DataFrame:
    # Parse the FRED CSV
    df = pd.read_csv(io.StringIO(csv_text))
    # Expect columns: DATE, JTSJOL
    # Convert date and numeric value, coerce '.' to NaN
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["value"] = pd.to_numeric(df["JTSJOL"], errors="coerce")

    # Keep 2020-01-01 and later, non-null
    df = df[df["DATE"] >= pd.Timestamp("2020-01-01")].copy()
    df = df.dropna(subset=["value"]).reset_index(drop=True)

    if df.empty:
        raise RuntimeError("No rows remain after filtering from 2020-01-01.")

    base = df.loc[df.index[0], "value"]  # first value at/after 2020-01-01
    df["month_end"] = df["DATE"].dt.strftime("%Y-%m-%d")
    df["JOLTS"] = (df["value"] / base * 100).round(2)
    df["type"] = "JOLTS"

    # Final columns similar to your Observable table
    return df[["month_end", "value", "JOLTS", "type"]]

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        csv_text = fetch_csv(FRED_CSV_URL)
    except Exception as e:
        print(f"ERROR: Failed to fetch FRED CSV: {e}", file=sys.stderr)
        sys.exit(1)

    # Write raw CSV exactly as returned (for reproducibility)
    RAW_PATH.write_text(csv_text, encoding="utf-8")

    # Also write a processed version (index since 2020-01-01)
    processed = to_processed(csv_text)
    processed.to_csv(PROC_PATH, index=False)

    print(f"Wrote: {RAW_PATH}")
    print(f"Wrote: {PROC_PATH}")
    print(f"Rows (processed): {len(processed)}")

if __name__ == "__main__":
    main()
