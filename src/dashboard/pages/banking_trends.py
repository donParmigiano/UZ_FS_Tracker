"""Banking sector trends page."""

import streamlit as st

from src.dashboard.charts import line_chart


def render(bank_filtered, selected_indicator: str):
    """Render banking indicator time series chart."""
    st.subheader("Banking sector trends")
    if selected_indicator in bank_filtered.columns:
        ts = bank_filtered.groupby("date", as_index=False)[selected_indicator].sum()
        st.plotly_chart(
            line_chart(ts, "date", selected_indicator, title=f"Sector trend: {selected_indicator}"),
            use_container_width=True,
        )
