import io
import sys
from pathlib import Path
import pandas as pd
import requests

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=JTSJOL"
DATA_DIR = Path("data")
RAW_PATH = DATA_DIR / "jtsjol_raw.csv"
PROC_PATH = DATA_DIR / "jtsjol_processed.csv"


def fetch_csv(url: str) -> str:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def to_processed(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(csv_text))

    # Rename columns to standard names
    df.rename(columns={"observation_date": "DATE", "JTSJOL": "value"}, inplace=True)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Filter from Jan 2020 onward and drop missing values
    df = df[df["DATE"] >= pd.Timestamp("2020-01-01")].dropna(subset=["value"]).copy()

    if df.empty:
        raise RuntimeError("No valid data available after filtering from 2020-01-01.")

    base_value = df.iloc[0]["value"]

    df["month"] = df["DATE"].dt.to_period("M").astype(str)
    df["month_end"] = df["DATE"] + pd.offsets.MonthEnd(0)
    df["month_end"] = df["month_end"].dt.strftime("%Y-%m-%d")

    df["JOLTS"] = (df["value"] / base_value * 100).round(2)
    df["type"] = "JOLTS"

    return df[["month", "month_end", "value", "JOLTS", "type"]]


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        csv_text = fetch_csv(FRED_CSV_URL)
    except Exception as e:
        print(f"❌ Failed to fetch FRED CSV: {e}", file=sys.stderr)
        sys.exit(1)

    RAW_PATH.write_text(csv_text, encoding="utf-8")

    try:
        processed_df = to_processed(csv_text)
        processed_df.to_csv(PROC_PATH, index=False)
    except Exception as e:
        print(f"❌ Failed to process CSV: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"✅ Wrote raw CSV: {RAW_PATH}")
    print(f"✅ Wrote processed CSV: {PROC_PATH}")
    print(f"✅ Rows processed: {len(processed_df)}")


if __name__ == "__main__":
    main()
