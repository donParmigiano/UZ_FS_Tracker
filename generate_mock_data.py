"""Generate synthetic Uzbekistan banking-sector data and load it into SQLite."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.constants import BANKS, DB_PATH, REGIONS
from src.database.schema import CREATE_BANK_MONTHLY, CREATE_DATA_CATALOGUE, CREATE_REGION_MONTHLY


def monthly_dates(periods: int = 24) -> pd.DatetimeIndex:
    return pd.date_range(end=pd.Timestamp.today().to_period("M").to_timestamp(), periods=periods, freq="MS")


def create_bank_data() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for date in monthly_dates():
        t = (date.year - 2024) * 12 + date.month
        for i, bank in enumerate(BANKS):
            scale = 1.0 + i * 0.08
            assets = (11_000 + 220 * t) * scale + rng.normal(0, 250)
            loans = assets * (0.56 + rng.uniform(-0.05, 0.05))
            deposits = assets * (0.62 + rng.uniform(-0.05, 0.05))
            capital = assets * (0.14 + rng.uniform(-0.01, 0.015))
            net_profit = assets * (0.017 + rng.uniform(-0.005, 0.005))
            corporate_loans = loans * (0.46 + rng.uniform(-0.05, 0.05))
            retail_loans = loans * (0.34 + rng.uniform(-0.04, 0.04))
            sme_loans = max(loans - corporate_loans - retail_loans, loans * 0.15)
            rows.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "bank": bank,
                    "assets": round(assets, 2),
                    "loans": round(loans, 2),
                    "deposits": round(deposits, 2),
                    "capital": round(capital, 2),
                    "net_profit": round(net_profit, 2),
                    "roa": round(rng.uniform(1.1, 2.5), 2),
                    "roe": round(rng.uniform(11, 23), 2),
                    "npl_ratio": round(rng.uniform(1.8, 6.2), 2),
                    "cost_to_income_ratio": round(rng.uniform(34, 63), 2),
                    "corporate_loans": round(corporate_loans, 2),
                    "retail_loans": round(retail_loans, 2),
                    "sme_loans": round(sme_loans, 2),
                    "deposit_rate": round(rng.uniform(14, 24), 2),
                    "loan_rate": round(rng.uniform(18, 31), 2),
                    "payment_transactions": round(rng.uniform(2_500_000, 16_000_000), 0),
                }
            )
    return pd.DataFrame(rows)


def create_region_data() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows = []
    for date in monthly_dates():
        t = (date.year - 2024) * 12 + date.month
        for i, region in enumerate(REGIONS):
            scale = 1 + (i * 0.05)
            rows.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "region": region,
                    "deposits": round((6_500 + 140 * t) * scale + rng.normal(0, 110), 2),
                    "loans": round((5_100 + 130 * t) * scale + rng.normal(0, 90), 2),
                    "active_cards": int((780_000 + 7_500 * t) * scale + rng.normal(0, 2000)),
                    "payment_volume": round((2_200 + 95 * t) * scale + rng.normal(0, 60), 2),
                }
            )
    return pd.DataFrame(rows)


def create_catalogue() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "dataset": "bank_monthly",
                "level": "bank-month",
                "description": "Synthetic monthly indicators for 12 Uzbekistan banks",
                "is_synthetic": 1,
            },
            {
                "dataset": "region_monthly",
                "level": "region-month",
                "description": "Synthetic monthly indicators for 14 Uzbekistan regions",
                "is_synthetic": 1,
            },
        ]
    )


def write_sqlite(bank_df: pd.DataFrame, region_df: pd.DataFrame, catalog_df: pd.DataFrame) -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_BANK_MONTHLY)
        conn.execute(CREATE_REGION_MONTHLY)
        conn.execute(CREATE_DATA_CATALOGUE)
        bank_df.to_sql("bank_monthly", conn, if_exists="replace", index=False)
        region_df.to_sql("region_monthly", conn, if_exists="replace", index=False)
        catalog_df.to_sql("data_catalogue", conn, if_exists="replace", index=False)


def main() -> None:
    bank_df = create_bank_data()
    region_df = create_region_data()
    catalog_df = create_catalogue()
    write_sqlite(bank_df, region_df, catalog_df)
    print(f"Synthetic dataset generated at: {DB_PATH}")
    print(f"bank_monthly rows: {len(bank_df)}")
    print(f"region_monthly rows: {len(region_df)}")


if __name__ == "__main__":
    main()
