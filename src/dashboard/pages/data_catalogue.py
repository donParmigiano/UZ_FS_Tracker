"""Data catalogue page."""

import streamlit as st


def render(catalog_df):
    """Render synthetic dataset catalogue table."""
    st.subheader("Data catalogue")
    st.info("All datasets listed below are synthetic and generated locally for demo use.")
    st.dataframe(catalog_df, use_container_width=True)
