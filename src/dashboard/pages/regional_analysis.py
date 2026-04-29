"""Regional analysis page."""

import streamlit as st

from src.analytics.metrics import latest_snapshot
from src.dashboard.charts import bar_chart, heatmap


def render(region_filtered, selected_indicator: str):
    """Render regional comparison and heatmap charts."""
    st.subheader("Regional analysis")
    regional_metric = selected_indicator if selected_indicator in region_filtered.columns else "deposits"
    latest_region = latest_snapshot(region_filtered)

    st.plotly_chart(
        bar_chart(
            latest_region.sort_values(regional_metric, ascending=False),
            "region",
            regional_metric,
            title=f"Regional comparison: {regional_metric}",
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        heatmap(region_filtered, "date", "region", regional_metric, title=f"Heatmap: {regional_metric}"),
        use_container_width=True,
    )
