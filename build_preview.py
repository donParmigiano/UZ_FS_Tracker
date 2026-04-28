"""Build a static HTML preview page from synthetic dashboard data."""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.io as pio

from src.common.constants import DB_PATH
from src.common.db import db_exists, read_table

OUT_PATH = Path("preview.html")


def figure_html(fig, title: str) -> str:
    return f"<h2>{title}</h2>" + pio.to_html(fig, include_plotlyjs="cdn", full_html=False)


def main() -> None:
    if not db_exists():
        print("Database not found. Run: python generate_mock_data.py")
        return

    bank_df = read_table("bank_monthly")
    region_df = read_table("region_monthly")
    bank_df["date"] = pd.to_datetime(bank_df["date"])
    region_df["date"] = pd.to_datetime(region_df["date"])

    latest_date = bank_df["date"].max()
    latest_banks = bank_df[bank_df["date"] == latest_date].sort_values("assets", ascending=False)
    trend = bank_df.groupby("date", as_index=False)["assets"].sum()
    regions = region_df[region_df["date"] == region_df["date"].max()].sort_values("deposits", ascending=False)

    fig1 = px.bar(latest_banks, x="bank", y="assets", title="Latest Assets by Bank")
    fig2 = px.line(trend, x="date", y="assets", title="Sector Assets Trend")
    fig3 = px.bar(regions, x="region", y="deposits", title="Regional Deposits Comparison")

    html = """
    <html><head><meta charset='utf-8'><title>UZ Banking Demo Preview</title></head>
    <body style='font-family: Arial; margin: 20px;'>
      <h1>Uzbekistan Banking Dashboard Preview</h1>
      <p><b>Important:</b> All data shown is synthetic demo data for Uzbekistan only.</p>
      <p>Database: {db_path}</p>
    """.format(db_path=DB_PATH)
    html += figure_html(fig1, "Bank Rankings")
    html += figure_html(fig2, "Banking Sector Trends")
    html += figure_html(fig3, "Regional Analysis")
    html += "</body></html>"

    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"Preview built at: {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
