# fetch_jolts.py
import sys
from pathlib import Path
import requests

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=JTSJOL"
DATA_DIR = Path("data")
RAW_PATH = DATA_DIR / "jtsjol_raw.csv"

def fetch_csv(url: str) -> str:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.text

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        csv_text = fetch_csv(FRED_CSV_URL)
    except Exception as e:
        print(f"ERROR: Failed to fetch FRED CSV: {e}", file=sys.stderr)
        sys.exit(1)

    RAW_PATH.write_text(csv_text, encoding="utf-8")
    print(f"Wrote: {RAW_PATH}")

if __name__ == "__main__":
    main()
