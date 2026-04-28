# Uzbekistan Banking and Financial Sector Intelligence Dashboard (MVP)

> **Synthetic demo only (core dashboard):** The base MVP dashboard data is locally generated synthetic data for Uzbekistan only.

## Overview
This project provides an offline-first Streamlit dashboard foundation for Uzbekistan banking and financial sector intelligence, plus a controlled April 2026 CBU banking statistics collection test.

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

## Controlled CBU test collection (April 2026 only)
Run:

```bash
python collect_cbu_bankstats_test.py
```

This controlled test script will:
- visit the April 2026 CBU bankstats filtered page,
- discover report pages,
- download `.xlsx`/`.xls` files,
- save raw files under `data/raw/cbu_bankstats/2026_04/`,
- create/update SQLite table `cbu_bankstats_sources`,
- export inventory CSV `data/processed/cbu_bankstats_inventory_2026_04.csv`,
- parse sheet-level CSVs under `data/processed/cbu_bankstats/2026_04/`,
- export parsing summary CSV `data/processed/cbu_bankstats_parse_summary_2026_04.csv`.

## Run the dashboard

```bash
python -m streamlit run app.py
```

If synthetic SQLite data is missing, the app will show instruction:

```bash
python generate_mock_data.py
```

## Build HTML preview

```bash
python build_preview.py
```

This creates `preview.html` so you can quickly open a visual demo in a browser without launching Streamlit.

## Project structure

```text
app.py
build_preview.py
collect_cbu_bankstats_test.py
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
- Core dashboard metrics are synthetic and not suitable for policy, investment, or risk decisions.
- CBU collection module is a controlled pilot for **April 2026 only**.
- No bank-website scraping is included.
- No production ETL orchestration or monitoring yet.

## Next steps for live Central Bank of Uzbekistan data
- Expand the CBU collection module from one-month pilot to parameterized monthly ingestion.
- Add stronger schema mapping and data validation in `src/parsers/`.
- Add data quality checks, retries, and idempotent upserts.
- Add metadata lineage and update SLAs in `config/sources.yaml`.
- Add authentication, role-based access, and export APIs.
