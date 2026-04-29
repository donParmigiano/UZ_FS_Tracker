"""Executive overview dashboard page."""

import streamlit as st

from src.analytics.metrics import latest_snapshot, sector_kpis
from src.dashboard.charts import bar_chart


def render(bank_filtered):
    """Render executive overview KPIs and latest assets chart."""
    st.subheader("Executive overview")
    kpis = sector_kpis(bank_filtered)
    cols = st.columns(4)
    for i, (label, value) in enumerate(kpis.items()):
        cols[i].metric(label, f"{value:,.0f}")

    latest = latest_snapshot(bank_filtered)
    st.plotly_chart(
        bar_chart(
            latest.sort_values("assets", ascending=False),
            "bank",
            "assets",
            title="Latest assets by bank",
        ),
        use_container_width=True,
    )
