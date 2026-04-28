"""Minimal Streamlit router for the Uzbekistan banking dashboard."""

import pandas as pd
import streamlit as st

from src.common.constants import BANK_INDICATORS, DB_PATH, REGION_INDICATORS
from src.common.db import db_exists, read_table
from src.dashboard.pages import PAGE_SPECS, cbu_bankstats_ytd


def load_dataframes() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load dashboard tables from SQLite and normalize date columns."""
    bank_df = read_table("bank_monthly")
    region_df = read_table("region_monthly")
    catalog_df = read_table("data_catalogue")

    bank_df["date"] = pd.to_datetime(bank_df["date"])
    region_df["date"] = pd.to_datetime(region_df["date"])
    return bank_df, region_df, catalog_df


def sidebar_filters(bank_df: pd.DataFrame, region_df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp, list[str], list[str], str]:
    """Render shared sidebar filters and return selected values."""
    st.sidebar.header("Filters")

    min_date = min(bank_df["date"].min(), region_df["date"].min())
    max_date = max(bank_df["date"].max(), region_df["date"].max())
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    selected_banks = st.sidebar.multiselect(
        "Bank",
        options=sorted(bank_df["bank"].unique()),
        default=sorted(bank_df["bank"].unique())[:4],
    )
    selected_regions = st.sidebar.multiselect(
        "Region",
        options=sorted(region_df["region"].unique()),
        default=sorted(region_df["region"].unique())[:4],
    )
    selected_indicator = st.sidebar.selectbox("Indicator", options=BANK_INDICATORS + REGION_INDICATORS)

    if len(date_range) == 2:
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    else:
        start_date, end_date = min_date, max_date

    return start_date, end_date, selected_banks, selected_regions, selected_indicator


def filter_data(
    bank_df: pd.DataFrame,
    region_df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    selected_banks: list[str],
    selected_regions: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply common date/bank/region filters."""
    bank_filtered = bank_df[(bank_df["date"] >= start_date) & (bank_df["date"] <= end_date)]
    region_filtered = region_df[(region_df["date"] >= start_date) & (region_df["date"] <= end_date)]

    if selected_banks:
        bank_filtered = bank_filtered[bank_filtered["bank"].isin(selected_banks)]
    if selected_regions:
        region_filtered = region_filtered[region_filtered["region"].isin(selected_regions)]

    return bank_filtered, region_filtered


def render_tabs(context: dict) -> None:
    """Render tabs from the shared page registry plus YTD CBU tab."""
    base_specs = [spec for spec in PAGE_SPECS if spec.title != "CBU Banking Stats YTD"]
    tab_labels = [spec.title for spec in base_specs] + ["CBU Banking Stats YTD"]
    tabs = st.tabs(tab_labels)

    for tab, spec in zip(tabs[:-1], base_specs):
        with tab:
            spec.render(context)

    with tabs[-1]:
        cbu_bankstats_ytd.render()


def main() -> None:
    """Run the Streamlit app."""
    st.set_page_config(page_title="UZ Banking Intelligence Dashboard", layout="wide")
    st.title("Uzbekistan Banking & Financial Sector Intelligence Dashboard")
    st.warning("⚠️ All data shown in this MVP is synthetic demo data for Uzbekistan only.")

    if not db_exists():
        st.error("Database not found. Please run: `python generate_mock_data.py`")
        st.stop()

    bank_df, region_df, catalog_df = load_dataframes()
    start_date, end_date, selected_banks, selected_regions, selected_indicator = sidebar_filters(bank_df, region_df)
    bank_filtered, region_filtered = filter_data(
        bank_df,
        region_df,
        start_date,
        end_date,
        selected_banks,
        selected_regions,
    )

    context = {
        "bank_filtered": bank_filtered,
        "region_filtered": region_filtered,
        "catalog_df": catalog_df,
        "selected_indicator": selected_indicator,
    }
    render_tabs(context)

    st.caption(f"SQLite database path: {DB_PATH}")


if __name__ == "__main__":
    main()
