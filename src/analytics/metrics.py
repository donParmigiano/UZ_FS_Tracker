"""Simple metric utilities used by dashboard and preview build."""

import pandas as pd


def latest_snapshot(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df.empty:
        return df
    latest_date = pd.to_datetime(df[date_col]).max()
    return df[pd.to_datetime(df[date_col]) == latest_date].copy()


def sector_kpis(bank_df: pd.DataFrame) -> dict:
    latest = latest_snapshot(bank_df)
    if latest.empty:
        return {}
    return {
        "Total assets": latest["assets"].sum(),
        "Total loans": latest["loans"].sum(),
        "Total deposits": latest["deposits"].sum(),
        "Total payment txns": latest["payment_transactions"].sum(),
    }
