"""CBU 2026 YTD collector outputs dashboard page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

SUMMARY_PATH = "data/processed/cbu_bankstats_parse_summary_2026_ytd.csv"
INVENTORY_PATH = "data/processed/cbu_bankstats_inventory_2026_ytd.csv"
QA_PATH = "data/processed/cbu_bankstats_parse_qa_2026_ytd.csv"
MASTER_PATH = "data/master/cbu_bankstats_cells_master_2026_ytd.csv"


def _safe_read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def render() -> None:
    """Render YTD summary metrics, inventory/QA tables, and master preview."""
    st.subheader("CBU Banking Stats YTD")
    st.caption("This tab displays outputs from collect_cbu_bankstats_ytd.py.")

    summary_df = _safe_read_csv(SUMMARY_PATH)
    inventory_df = _safe_read_csv(INVENTORY_PATH)
    qa_df = _safe_read_csv(QA_PATH)
    master_df = _safe_read_csv(MASTER_PATH)

    if summary_df.empty and inventory_df.empty and qa_df.empty and master_df.empty:
        st.info(
            "YTD outputs are not available yet. "
            "Run `python collect_cbu_bankstats_ytd.py` first."
        )
        return

    if not summary_df.empty:
        monthly = summary_df[summary_df["month"] != "TOTAL"].copy()
        total = summary_df[summary_df["month"] == "TOTAL"].head(1)

        if "excel_files_found" in monthly:
            months_processed = int((monthly["excel_files_found"] > 0).sum())
        else:
            months_processed = len(monthly)

        reports_found = (
            int(total["reports_found"].iloc[0])
            if not total.empty
            else int(monthly.get("reports_found", pd.Series(dtype=int)).sum())
        )
        files_downloaded = (
            int(total["excel_files_downloaded"].iloc[0])
            if not total.empty
            else int(monthly.get("excel_files_downloaded", pd.Series(dtype=int)).sum())
        )
        parsed_csv_files = (
            int(total["parsed_csv_files"].iloc[0])
            if not total.empty
            else int(monthly.get("parsed_csv_files", pd.Series(dtype=int)).sum())
        )
        if not total.empty and "master_row_count" in total.columns:
            master_row_count = int(total["master_row_count"].iloc[0])
        else:
            master_row_count = len(master_df)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Months processed", f"{months_processed}")
        c2.metric("Reports found", f"{reports_found}")
        c3.metric("Excel downloaded", f"{files_downloaded}")
        c4.metric("Parsed CSV files", f"{parsed_csv_files}")
        c5.metric("Master row count", f"{master_row_count}")

        st.markdown("**YTD parse summary**")
        st.dataframe(summary_df, use_container_width=True)

    if not inventory_df.empty:
        st.markdown("**YTD source inventory**")
        st.dataframe(inventory_df, use_container_width=True)

    if not qa_df.empty:
        st.markdown("**YTD parse QA**")
        st.dataframe(qa_df, use_container_width=True)

    if not master_df.empty:
        st.markdown("**Master preview**")
        if "month" in master_df:
            month_options = ["All"] + sorted(
                master_df["month"].dropna().astype(str).unique().tolist()
            )
        else:
            month_options = ["All"]

        if "report_title" in master_df:
            title_options = ["All"] + sorted(
                master_df["report_title"].dropna().astype(str).unique().tolist()
            )
        else:
            title_options = ["All"]

        if "sheet_name" in master_df:
            sheet_options = ["All"] + sorted(
                master_df["sheet_name"].dropna().astype(str).unique().tolist()
            )
        else:
            sheet_options = ["All"]

        f1, f2, f3 = st.columns(3)
        selected_month = f1.selectbox(
            "Filter month", options=month_options, key="ytd_master_month"
        )
        selected_title = f2.selectbox(
            "Filter report title", options=title_options, key="ytd_master_title"
        )
        selected_sheet = f3.selectbox(
            "Filter sheet name", options=sheet_options, key="ytd_master_sheet"
        )

        filtered = master_df.copy()
        if selected_month != "All" and "month" in filtered:
            filtered = filtered[filtered["month"].astype(str) == selected_month]
        if selected_title != "All" and "report_title" in filtered:
            filtered = filtered[filtered["report_title"].astype(str) == selected_title]
        if selected_sheet != "All" and "sheet_name" in filtered:
            filtered = filtered[filtered["sheet_name"].astype(str) == selected_sheet]

        st.dataframe(filtered.head(2000), use_container_width=True)
