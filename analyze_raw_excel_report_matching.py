"""Analyze raw Excel inventory and audit report-key matching coverage."""

from __future__ import annotations

import pandas as pd

from src.common.report_matching import is_html_fallback, match_report_key


INPUT_CSV = "data/processed/raw_excel_inventory.csv"
AUDIT_OUTPUT_CSV = "data/processed/raw_report_matching_audit.csv"
SUMMARY_OUTPUT_CSV = "data/processed/raw_report_matching_summary.csv"


def main() -> None:
    df = pd.read_csv(INPUT_CSV)

    match_results = df["file_name"].fillna("").apply(match_report_key)
    match_df = pd.DataFrame(match_results.tolist())

    out_df = pd.concat([df, match_df], axis=1)
    out_df["is_html_fallback"] = out_df["file_name"].fillna("").apply(is_html_fallback)

    audit_columns = [
        "period_folder",
        "file_name",
        "sheet_name",
        "max_row",
        "max_column",
        "normalized_name",
        "report_key",
        "matched_alias",
        "match_confidence",
        "is_html_fallback",
        "notes",
    ]
    audit_df = out_df[audit_columns].copy()
    audit_df.to_csv(AUDIT_OUTPUT_CSV, index=False)

    # Compute summary at file granularity (one row per period+file).
    file_level = audit_df.drop_duplicates(subset=["period_folder", "file_name"]).copy()

    summary_df = (
        file_level.groupby("report_key", dropna=False)
        .agg(
            file_count=("file_name", "count"),
            period_count=("period_folder", "nunique"),
            first_period=("period_folder", "min"),
            last_period=("period_folder", "max"),
            html_fallback_file_count=("is_html_fallback", "sum"),
            unknown_file_count=("report_key", lambda s: int((s == "unknown").sum())),
        )
        .reset_index()
    )

    sheet_counts = (
        audit_df.groupby("report_key", dropna=False)["sheet_name"].count().rename("sheet_count").reset_index()
    )
    summary_df = summary_df.merge(sheet_counts, on="report_key", how="left")

    summary_df = summary_df[
        [
            "report_key",
            "file_count",
            "sheet_count",
            "period_count",
            "first_period",
            "last_period",
            "html_fallback_file_count",
            "unknown_file_count",
        ]
    ].sort_values("report_key")

    summary_df.to_csv(SUMMARY_OUTPUT_CSV, index=False)

    print(f"Wrote audit CSV: {AUDIT_OUTPUT_CSV}")
    print(f"Wrote summary CSV: {SUMMARY_OUTPUT_CSV}")


if __name__ == "__main__":
    main()
