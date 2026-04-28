"""Streamlit app router for Uzbekistan banking sector intelligence dashboard."""

import pandas as pd
import streamlit as st

from src.common.constants import BANK_INDICATORS, DB_PATH, REGION_INDICATORS
from src.common.db import db_exists, read_table
from src.dashboard.pages import (
    bank_rankings,
    banking_trends,
    cbu_april_test,
    data_catalogue,
    deposits_loans,
    executive,
    payments_digital,
    regional_analysis,
)

st.set_page_config(page_title="UZ Banking Intelligence Dashboard", layout="wide")
st.title("Uzbekistan Banking & Financial Sector Intelligence Dashboard")
st.warning("⚠️ All data shown in this MVP is synthetic demo data for Uzbekistan only.")

if not db_exists():
    st.error("Database not found. Please run: `python generate_mock_data.py`")
    st.stop()

bank_df = read_table("bank_monthly")
region_df = read_table("region_monthly")
catalog_df = read_table("data_catalogue")

bank_df["date"] = pd.to_datetime(bank_df["date"])
region_df["date"] = pd.to_datetime(region_df["date"])

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

bank_filtered = bank_df[(bank_df["date"] >= start_date) & (bank_df["date"] <= end_date)]
region_filtered = region_df[(region_df["date"] >= start_date) & (region_df["date"] <= end_date)]

if selected_banks:
    bank_filtered = bank_filtered[bank_filtered["bank"].isin(selected_banks)]
if selected_regions:
    region_filtered = region_filtered[region_filtered["region"].isin(selected_regions)]

TAB_LABELS = [
    "Executive overview",
    "Banking sector trends",
    "Bank rankings",
    "Regional analysis",
    "Deposits and loans",
    "Payments and digital finance",
    "Data catalogue",
    "CBU Banking Stats Test",
]

pages = st.tabs(TAB_LABELS)

with pages[0]:
    executive.render(bank_filtered)
with pages[1]:
    banking_trends.render(bank_filtered, selected_indicator)
with pages[2]:
    bank_rankings.render(bank_filtered)
with pages[3]:
    regional_analysis.render(region_filtered, selected_indicator)
with pages[4]:
    deposits_loans.render(bank_filtered)
with pages[5]:
    payments_digital.render(bank_filtered, region_filtered)
with pages[6]:
    data_catalogue.render(catalog_df)
with pages[7]:
    cbu_april_test.render()

st.caption(f"SQLite database path: {DB_PATH}")
