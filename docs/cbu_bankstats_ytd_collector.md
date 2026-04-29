# CBU Banking Stats YTD Collector (2026 Jan-Apr)

This guide documents the standalone YTD collector script:

- `collect_cbu_bankstats_ytd.py`

## Scope
The collector targets the CBU bankstats section filter `arFilter_ff[SECTION_ID]=3497` for:
- `2026_01`
- `2026_02`
- `2026_03`
- `2026_04`

## Run
```bash
python collect_cbu_bankstats_ytd.py
```

Optional flags:
```bash
python collect_cbu_bankstats_ytd.py --parse-only
python collect_cbu_bankstats_ytd.py --overwrite
```

## Outputs
- Raw Excel files: `data/raw/cbu_bankstats/YYYY_MM/`
- Parsed CSV sheets: `data/processed/cbu_bankstats/YYYY_MM/`
- Inventory: `data/processed/cbu_bankstats_inventory_2026_ytd.csv`
- Parse summary: `data/processed/cbu_bankstats_parse_summary_2026_ytd.csv`
- Parse QA: `data/processed/cbu_bankstats_parse_qa_2026_ytd.csv`
- Master cells: `data/master/cbu_bankstats_cells_master_2026_ytd.csv`

## Dashboard
Open `streamlit run app.py` and use **CBU Banking Stats YTD**.
