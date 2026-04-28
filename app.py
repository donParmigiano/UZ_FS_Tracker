"""Streamlit app for Uzbekistan banking sector intelligence (synthetic MVP)."""

import pandas as pd
import streamlit as st

from src.analytics.metrics import latest_snapshot, sector_kpis
from src.common.constants import BANK_INDICATORS, DB_PATH, REGION_INDICATORS
from src.common.db import db_exists, read_table
from src.dashboard.charts import bar_chart, heatmap, line_chart, stacked_bar

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
date_range = st.sidebar.date_input("Date range", value=(min_date.date(), max_date.date()), min_value=min_date.date(), max_value=max_date.date())
selected_banks = st.sidebar.multiselect("Bank", options=sorted(bank_df["bank"].unique()), default=sorted(bank_df["bank"].unique())[:4])
selected_regions = st.sidebar.multiselect("Region", options=sorted(region_df["region"].unique()), default=sorted(region_df["region"].unique())[:4])
selected_indicator = st.sidebar.selectbox("Indicator", options=BANK_INDICATORS + REGION_INDICATORS)

if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
else:
    start_date = min_date
    end_date = max_date

bank_filtered = bank_df[(bank_df["date"] >= start_date) & (bank_df["date"] <= end_date)]
region_filtered = region_df[(region_df["date"] >= start_date) & (region_df["date"] <= end_date)]
if selected_banks:
    bank_filtered = bank_filtered[bank_filtered["bank"].isin(selected_banks)]
if selected_regions:
    region_filtered = region_filtered[region_filtered["region"].isin(selected_regions)]

tabs = st.tabs([
    "Executive overview",
    "Banking sector trends",
    "Bank rankings",
    "Regional analysis",
    "Deposits and loans",
    "Payments and digital finance",
    "Data catalogue",
])

with tabs[0]:
    st.subheader("Executive overview")
    kpis = sector_kpis(bank_filtered)
    cols = st.columns(4)
    for i, (label, value) in enumerate(kpis.items()):
        cols[i].metric(label, f"{value:,.0f}")
    latest = latest_snapshot(bank_filtered)
    st.plotly_chart(bar_chart(latest.sort_values("assets", ascending=False), "bank", "assets", title="Latest assets by bank"), use_container_width=True)

with tabs[1]:
    st.subheader("Banking sector trends")
    if selected_indicator in bank_filtered.columns:
        ts = bank_filtered.groupby("date", as_index=False)[selected_indicator].sum()
        st.plotly_chart(line_chart(ts, "date", selected_indicator, title=f"Sector trend: {selected_indicator}"), use_container_width=True)

with tabs[2]:
    st.subheader("Bank rankings")
    ranking_metric = st.selectbox("Ranking metric", options=["assets", "loans", "deposits", "net_profit", "payment_transactions"], key="ranking_metric")
    latest = latest_snapshot(bank_filtered)
    st.plotly_chart(bar_chart(latest.sort_values(ranking_metric, ascending=False), "bank", ranking_metric, title=f"Ranking by {ranking_metric}"), use_container_width=True)

with tabs[3]:
    st.subheader("Regional analysis")
    regional_metric = selected_indicator if selected_indicator in region_filtered.columns else "deposits"
    latest_r = latest_snapshot(region_filtered)
    st.plotly_chart(bar_chart(latest_r.sort_values(regional_metric, ascending=False), "region", regional_metric, title=f"Regional comparison: {regional_metric}"), use_container_width=True)
    st.plotly_chart(heatmap(region_filtered, "date", "region", regional_metric, title=f"Heatmap: {regional_metric}"), use_container_width=True)

with tabs[4]:
    st.subheader("Deposits and loans")
    loans_deposits = bank_filtered.groupby("date", as_index=False)[["deposits", "loans"]].sum().melt(id_vars="date", var_name="metric", value_name="value")
    st.plotly_chart(stacked_bar(loans_deposits, "date", "value", "metric", title="Sector deposits vs loans"), use_container_width=True)

with tabs[5]:
    st.subheader("Payments and digital finance")
    payments = bank_filtered.groupby("date", as_index=False)["payment_transactions"].sum()
    st.plotly_chart(line_chart(payments, "date", "payment_transactions", title="Payment transactions over time"), use_container_width=True)
    cards = region_filtered.groupby("date", as_index=False)["active_cards"].sum()
    st.plotly_chart(line_chart(cards, "date", "active_cards", title="Active cards over time"), use_container_width=True)

with tabs[6]:
    st.subheader("Data catalogue")
    st.info("All datasets listed below are synthetic and generated locally for demo use.")
    st.dataframe(catalog_df, use_container_width=True)

st.caption(f"SQLite database path: {DB_PATH}")
