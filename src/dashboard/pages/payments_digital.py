"""Payments and digital finance page."""

import streamlit as st

from src.dashboard.charts import line_chart


def render(bank_filtered, region_filtered):
    """Render payment transactions and active cards charts."""
    st.subheader("Payments and digital finance")

    payments = bank_filtered.groupby("date", as_index=False)["payment_transactions"].sum()
    st.plotly_chart(
        line_chart(payments, "date", "payment_transactions", title="Payment transactions over time"),
        use_container_width=True,
    )

    cards = region_filtered.groupby("date", as_index=False)["active_cards"].sum()
    st.plotly_chart(
        line_chart(cards, "date", "active_cards", title="Active cards over time"),
        use_container_width=True,
    )
