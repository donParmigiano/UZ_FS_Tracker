# Uzbekistan Banking and Financial Sector Intelligence Dashboard (MVP)

> **Synthetic demo only**: This MVP is strictly for Uzbekistan and uses locally generated synthetic data. It does **not** scrape external websites.

## Overview
This project provides an offline-first Streamlit dashboard foundation for Uzbekistan banking and financial sector intelligence.

## Installation
1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Generate synthetic mock data
Run:

```bash
python generate_mock_data.py
```

This creates SQLite database `data/database/uz_banking_demo.sqlite` with:
- 12 Uzbekistan banks
- 14 Uzbekistan regions
- 24 months of monthly data
- Banking indicators: assets, loans, deposits, capital, net profit, ROA, ROE, NPL ratio, cost-to-income ratio, corporate loans, retail loans, SME loans, deposit rates, loan rates, payment transactions
- Regional indicators: deposits, loans, active cards, payment volume

## Run the dashboard

```bash
streamlit run app.py
```

If the database is missing, the app will show a clear instruction to run:

```bash
python generate_mock_data.py
```

## April 2026 CBU banking stats test collector

Run the collector test:

```bash
python collect_cbu_bankstats_test.py
```

Optional modes:

```bash
python collect_cbu_bankstats_test.py --parse-only
python collect_cbu_bankstats_test.py --overwrite
```

Notes:
- Raw Excel files under `data/raw/cbu_bankstats/2026_04/` are the source of truth.
- Parsed CSV files under `data/processed/cbu_bankstats/2026_04/` are audit/review outputs.
- Parsed CSV exports keep high numeric precision (up to 12 decimal places).
- Dashboard tables apply display rounding in the UI separately; this does not change stored CSV outputs.
- Use `data/processed/cbu_bankstats_parse_qa_2026_04.csv` to validate sheet-level parsing QA metrics.

Then run the dashboard and open the **CBU Banking Stats Test** tab:

```bash
streamlit run app.py
```

## 2026 YTD CBU banking stats collector (Jan-Apr)

Run the year-to-date collector:

```bash
python collect_cbu_bankstats_ytd.py
```

Parse only existing raw files:

```bash
python collect_cbu_bankstats_ytd.py --parse-only
```

Overwrite existing raw/processed files:

```bash
python collect_cbu_bankstats_ytd.py --overwrite
```

Data layers:
- `data/raw/cbu_bankstats/YYYY_MM/`: raw Excel source files from CBU (source of truth).
- `data/processed/cbu_bankstats/YYYY_MM/`: parsed review CSV files and YTD inventory/summary/QA CSV outputs.
- `data/master/cbu_bankstats_cells_master_2026_ytd.csv`: technical cell-level master combining parsed outputs across Jan-Apr 2026.

## Build HTML preview

```bash
python build_preview.py
```

This creates `preview.html` so you can quickly open a visual demo in a browser without launching Streamlit.

## Project structure

```text
app.py
build_preview.py
generate_mock_data.py
requirements.txt
config/sources.yaml
data/raw/
data/processed/
data/database/
logs/
src/analytics/
src/collectors/
src/common/
src/dashboard/
src/database/
src/parsers/
```

## Limitations
- Data is synthetic and not suitable for policy, investment, or risk decisions.
- No live integrations with the Central Bank of Uzbekistan yet.
- No ETL orchestration or production monitoring in this MVP.

## Next steps for live Central Bank of Uzbekistan data
- Add official source connectors in `src/collectors/`.
- Build parser and validation logic in `src/parsers/`.
- Add incremental ingestion jobs and data quality checks.
- Introduce metadata lineage and update SLAs in `config/sources.yaml`.
- Add authentication, role-based access, and export APIs.
