"""Plotly chart builders for consistent visuals."""

import plotly.express as px


def line_chart(df, x, y, color=None, title=""):
    return px.line(df, x=x, y=y, color=color, title=title)


def bar_chart(df, x, y, color=None, title=""):
    return px.bar(df, x=x, y=y, color=color, title=title)


def stacked_bar(df, x, y, color, title=""):
    return px.bar(df, x=x, y=y, color=color, title=title, barmode="stack")


def heatmap(df, x, y, z, title=""):
    return px.density_heatmap(df, x=x, y=y, z=z, title=title)
