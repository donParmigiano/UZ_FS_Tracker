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
    "CBU Banking Stats Test",
    "CBU Banking Stats YTD",
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

with tabs[7]:
    st.subheader("CBU Banking Stats Test")
    inventory_path = "data/processed/cbu_bankstats_inventory_2026_04.csv"
    summary_path = "data/processed/cbu_bankstats_parse_summary_2026_04.csv"
    qa_path = "data/processed/cbu_bankstats_parse_qa_2026_04.csv"
    st.caption("This tab displays outputs from collect_cbu_bankstats_test.py.")

    def _ui_round(df: pd.DataFrame, digits: int = 4) -> pd.DataFrame:
        view_df = df.copy()
        numeric_cols = view_df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            view_df[numeric_cols] = view_df[numeric_cols].round(digits)
        return view_df

    try:
        summary_df = pd.read_csv(summary_path)
        st.markdown("**Parse summary**")
        st.dataframe(_ui_round(summary_df), use_container_width=True)
    except Exception:
        st.info("Parse summary not found yet. Run `python collect_cbu_bankstats_test.py` first.")

    try:
        inventory_df = pd.read_csv(inventory_path)
        st.markdown("**Source inventory**")
        st.dataframe(_ui_round(inventory_df), use_container_width=True)
    except Exception:
        st.info("Inventory file not found yet. Run `python collect_cbu_bankstats_test.py` first.")

    try:
        qa_df = pd.read_csv(qa_path)
        st.markdown("**Parse QA report**")
        st.dataframe(_ui_round(qa_df), use_container_width=True)
    except Exception:
        st.info("Parse QA report not found yet. Run `python collect_cbu_bankstats_test.py` first.")

with tabs[8]:
    st.subheader("CBU Banking Stats YTD")
    ytd_inventory_path = "data/processed/cbu_bankstats_inventory_2026_ytd.csv"
    ytd_summary_path = "data/processed/cbu_bankstats_parse_summary_2026_ytd.csv"
    ytd_qa_path = "data/processed/cbu_bankstats_parse_qa_2026_ytd.csv"
    ytd_master_path = "data/master/cbu_bankstats_cells_master_2026_ytd.csv"
    st.caption("This tab displays outputs from collect_cbu_bankstats_ytd.py.")

    def _ui_round(df: pd.DataFrame, digits: int = 4) -> pd.DataFrame:
        view_df = df.copy()
        numeric_cols = view_df.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            view_df[numeric_cols] = view_df[numeric_cols].round(digits)
        return view_df

    ytd_inventory_df = None
    ytd_qa_df = None
    ytd_master_df = None

    try:
        ytd_summary_df = pd.read_csv(ytd_summary_path)
        st.markdown("**YTD parse summary**")
        st.dataframe(_ui_round(ytd_summary_df), use_container_width=True)
    except Exception:
        st.info("YTD parse summary not found yet. Run `python collect_cbu_bankstats_ytd.py` first.")

    metrics_cols = st.columns(5)

    try:
        ytd_inventory_df = pd.read_csv(ytd_inventory_path)
        months_processed = ytd_inventory_df["period"].nunique() if "period" in ytd_inventory_df.columns else 0
        reports_found = ytd_inventory_df["report_url"].replace("", pd.NA).dropna().nunique() if "report_url" in ytd_inventory_df.columns else 0
        excel_downloaded = ytd_inventory_df["download_status"].isin(["downloaded", "skipped_existing", "existing_raw"]).sum() if "download_status" in ytd_inventory_df.columns else 0
        parsed_csv_files = int(ytd_inventory_df["parsed_csv_files"].fillna(0).sum()) if "parsed_csv_files" in ytd_inventory_df.columns else 0
        metrics_cols[0].metric("Months processed", f"{months_processed}")
        metrics_cols[1].metric("Reports found", f"{reports_found}")
        metrics_cols[2].metric("Excel files downloaded", f"{excel_downloaded}")
        metrics_cols[3].metric("Parsed CSV files", f"{parsed_csv_files}")
        st.markdown("**YTD source inventory**")
        st.dataframe(_ui_round(ytd_inventory_df), use_container_width=True)
    except Exception:
        st.info("YTD inventory file not found yet. Run `python collect_cbu_bankstats_ytd.py` first.")

    try:
        ytd_qa_df = pd.read_csv(ytd_qa_path)
        st.markdown("**YTD parse QA**")
        st.dataframe(_ui_round(ytd_qa_df), use_container_width=True)
    except Exception:
        st.info("YTD parse QA not found yet. Run `python collect_cbu_bankstats_ytd.py` first.")

    try:
        ytd_master_df = pd.read_csv(ytd_master_path)
        metrics_cols[4].metric("Master row count", f"{len(ytd_master_df):,}")
        st.markdown("**YTD technical master preview**")
        filter_cols = st.columns(3)
        month_options = ["All"] + sorted(ytd_master_df["period"].dropna().astype(str).unique().tolist()) if "period" in ytd_master_df.columns else ["All"]
        report_options = ["All"] + sorted(ytd_master_df["report_title"].dropna().astype(str).unique().tolist()) if "report_title" in ytd_master_df.columns else ["All"]
        sheet_options = ["All"] + sorted(ytd_master_df["sheet_name"].dropna().astype(str).unique().tolist()) if "sheet_name" in ytd_master_df.columns else ["All"]
        selected_month = filter_cols[0].selectbox("Month", options=month_options, key="ytd_month_filter")
        selected_report = filter_cols[1].selectbox("Report title", options=report_options, key="ytd_report_filter")
        selected_sheet = filter_cols[2].selectbox("Sheet", options=sheet_options, key="ytd_sheet_filter")

        preview_df = ytd_master_df.copy()
        if selected_month != "All":
            preview_df = preview_df[preview_df["period"] == selected_month]
        if selected_report != "All":
            preview_df = preview_df[preview_df["report_title"] == selected_report]
        if selected_sheet != "All":
            preview_df = preview_df[preview_df["sheet_name"] == selected_sheet]
        st.dataframe(preview_df.head(1000), use_container_width=True)
    except Exception:
        st.info("YTD master file not found yet. Run `python collect_cbu_bankstats_ytd.py` first.")

st.caption(f"SQLite database path: {DB_PATH}")
