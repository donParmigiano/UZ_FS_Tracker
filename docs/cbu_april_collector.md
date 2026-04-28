# CBU April 2026 Collector Guide

This guide documents the April 2026 collector test script used by the dashboard's **CBU Banking Stats Test** tab.

## Script
- `collect_cbu_bankstats_test.py`

## What it does
1. Crawls the April 2026 CBU banking statistics listing.
2. Extracts report-page links and Excel file links.
3. Downloads Excel files into `data/raw/cbu_bankstats/2026_04/`.
4. Parses each sheet to CSV under `data/processed/cbu_bankstats/2026_04/`.
5. Exports inventory and parse summary CSV files:
   - `data/processed/cbu_bankstats_inventory_2026_04.csv`
   - `data/processed/cbu_bankstats_parse_summary_2026_04.csv`

## Run
```bash
python collect_cbu_bankstats_test.py
```

## View in dashboard
```bash
streamlit run app.py
```
Then open **CBU Banking Stats Test**.

## Notes
- The script may fail in restricted environments where outbound requests to `https://cbu.uz` are blocked.
- This is a test collector and is intentionally simple for learning and iteration.
