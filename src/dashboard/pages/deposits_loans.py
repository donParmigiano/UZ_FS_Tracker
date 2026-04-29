"""Deposits and loans page."""

import streamlit as st

from src.dashboard.charts import stacked_bar


def render(bank_filtered):
    """Render stacked deposits vs loans chart."""
    st.subheader("Deposits and loans")
    loans_deposits = (
        bank_filtered.groupby("date", as_index=False)[["deposits", "loans"]]
        .sum()
        .melt(id_vars="date", var_name="metric", value_name="value")
    )
    st.plotly_chart(
        stacked_bar(loans_deposits, "date", "value", "metric", title="Sector deposits vs loans"),
        use_container_width=True,
    )
