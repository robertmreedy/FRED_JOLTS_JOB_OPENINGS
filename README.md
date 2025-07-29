# JOLTS (JTSJOL) Auto-Updated CSV

This repo fetches the FRED CSV for **Job Openings: Total Nonfarm (JTSJOL)** from:

- https://fred.stlouisfed.org/graph/fredgraph.csv?id=JTSJOL

Outputs:
- `data/jtsjol_raw.csv` — exact copy of the FRED CSV response
- `data/jtsjol_processed.csv` — filtered to 2020-01-01+, with:
  - `month_end` (YYYY-MM-DD)
  - `value` (original series value)
  - `JOLTS` (index, 2020-01 = 100)
  - `type` (constant "JOLTS")

A GitHub Action runs daily and commits if data changed.
