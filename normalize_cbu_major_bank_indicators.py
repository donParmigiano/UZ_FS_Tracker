"""Normalize CBU major bank indicators table into a long-form dataset.

This utility converts a wide CSV (period columns + metric columns) into a
standardized long format suitable for analytics pipelines.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_ID_COLUMNS = (
    "bank_name",
    "bank_code",
    "indicator",
    "unit",
    "source",
)


def _normalize_column_name(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace("%", "pct")
        .replace("/", "_")
        .replace("-", "_")
        .replace(" ", "_")
    )


def _coerce_numeric(series: pd.Series) -> pd.Series:
    text = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(text, errors="coerce")


def normalize_major_bank_indicators(
    dataframe: pd.DataFrame,
    id_columns: Iterable[str] = DEFAULT_ID_COLUMNS,
) -> pd.DataFrame:
    """Transform a wide major-bank-indicators table into long form."""
    normalized = dataframe.copy()
    normalized.columns = [_normalize_column_name(col) for col in normalized.columns]

    id_columns_normalized = [_normalize_column_name(col) for col in id_columns]
    available_id_columns = [col for col in id_columns_normalized if col in normalized.columns]
    value_columns = [col for col in normalized.columns if col not in available_id_columns]

    melted = normalized.melt(
        id_vars=available_id_columns,
        value_vars=value_columns,
        var_name="period",
        value_name="value",
    )

    melted["period"] = melted["period"].astype(str).str.strip()
    melted["value"] = _coerce_numeric(melted["value"])
    melted = melted.dropna(subset=["value"])

    order = available_id_columns + ["period", "value"]
    return melted[order].sort_values(order[:-1]).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize CBU major bank indicators from wide to long format."
    )
    parser.add_argument("input_csv", type=Path, help="Path to input CSV file")
    parser.add_argument("output_csv", type=Path, help="Path to output CSV file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source = pd.read_csv(args.input_csv)
    normalized = normalize_major_bank_indicators(source)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(args.output_csv, index=False)

    print(
        f"Normalized {len(source)} source rows into {len(normalized)} output rows: "
        f"{args.output_csv}"
    )


if __name__ == "__main__":
    main()
