"""CBU April 2026 test collector outputs page."""

import pandas as pd
import streamlit as st

INVENTORY_PATH = "data/processed/cbu_bankstats_inventory_2026_04.csv"
SUMMARY_PATH = "data/processed/cbu_bankstats_parse_summary_2026_04.csv"


def render() -> None:
    """Render CBU April collector summary and source inventory outputs."""
    st.subheader("CBU Banking Stats Test")
    st.caption("This tab displays outputs from collect_cbu_bankstats_test.py.")

    try:
        summary_df = pd.read_csv(SUMMARY_PATH)
        st.markdown("**Parse summary**")
        st.dataframe(summary_df, use_container_width=True)
    except Exception:
        st.info(
            "Parse summary not found yet. "
            "Run `python collect_cbu_bankstats_test.py` first."
        )

    try:
        inventory_df = pd.read_csv(INVENTORY_PATH)
        st.markdown("**Source inventory**")
        st.dataframe(inventory_df, use_container_width=True)
    except Exception:
        st.info(
            "Inventory file not found yet. "
            "Run `python collect_cbu_bankstats_test.py` first."
        )
