"""Bank rankings page."""

import streamlit as st

from src.analytics.metrics import latest_snapshot
from src.dashboard.charts import bar_chart


RANKING_OPTIONS = ["assets", "loans", "deposits", "net_profit", "payment_transactions"]


def render(bank_filtered):
    """Render latest bank ranking bar chart by selected metric."""
    st.subheader("Bank rankings")
    ranking_metric = st.selectbox("Ranking metric", options=RANKING_OPTIONS, key="ranking_metric")
    latest = latest_snapshot(bank_filtered)
    st.plotly_chart(
        bar_chart(
            latest.sort_values(ranking_metric, ascending=False),
            "bank",
            ranking_metric,
            title=f"Ranking by {ranking_metric}",
        ),
        use_container_width=True,
    )
