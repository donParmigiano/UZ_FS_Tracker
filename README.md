# Uzbekistan Banking and Financial Sector Intelligence Dashboard (MVP)

> **Synthetic demo only**: This MVP is scoped to Uzbekistan and uses locally generated synthetic data.

## Project purpose
This project provides a beginner-friendly Streamlit dashboard foundation for Uzbekistan banking and financial sector analytics, with offline mock data generation and modular dashboard pages.

## Quick start
1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Generate mock data:

```bash
python generate_mock_data.py
```

3. Run the dashboard:

```bash
streamlit run app.py
```

## Documentation
- [CBU April collector guide](docs/cbu_april_collector.md)
- [Data architecture guide](docs/data_architecture.md)

## High-level folder structure
```text
app.py
README.md
requirements.txt
config/sources.yaml
collect_cbu_bankstats_test.py
docs/
data/
src/
```
