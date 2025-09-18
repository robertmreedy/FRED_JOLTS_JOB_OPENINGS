import io
import sys
from pathlib import Path
import pandas as pd
import requests

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=FRBATLWGT3MMAUMHWGO&scale=left&cosd=1997-03-01&coed=2025-08-01&line_color=%230073e6&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2025-09-18&revision_date=2025-09-18&nd=1997-03-01"
DATA_DIR = Path("data")
RAW_PATH = DATA_DIR / "atlwage_raw.csv"
PROC_PATH = DATA_DIR / "atlwage_processed.csv"


def fetch_csv(url: str) -> str:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def to_processed(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(csv_text))

    # Rename columns to standard names
    df.rename(columns={"observation_date": "DATE", "FRBATLWGT3MMAUMHWGO": "value"}, inplace=True)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Filter from Jan 2019 onward and drop missing values
    df = df[df["DATE"] >= pd.Timestamp("2019-01-01")].dropna(subset=["value"]).copy()

    if df.empty:
        raise RuntimeError("No valid data available after filtering from 2019-01-01.")

    df["month"] = df["DATE"].dt.to_period("M").astype(str)
    df["3_month_wage_growth"] = (df["value"] / 100).round(3)

    return df[["month", "3_month_wage_growth"]]


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
