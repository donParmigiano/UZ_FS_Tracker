"""Build a simple coverage matrix for CBU report types.

This script reads report-matching output and produces one row per report_key with
coverage and prioritization metadata for normalization planning.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

INPUT_CSV = Path("data/processed/raw_report_matching_summary.csv")
OUTPUT_CSV = Path("data/processed/report_coverage_matrix.csv")

# Priority buckets requested for dashboard planning.
PRIORITY_1_KEYS = {
    "major_bank_indicators",
    "bank_npl",
    "bank_loans_deposits",
}
PRIORITY_2_KEYS = {
    "capital_adequacy",
    "profitability_indicators",
    "relative_bank_indicators",
}
PRIORITY_3_KEYS = {
    "regional_loans_deposits",
    "loan_types",
    "deposit_types",
    "balance_sheet",
    "financial_results",
}


def determine_priority(report_key: str) -> int:
    """Map report_key to dashboard priority."""
    if report_key == "unknown":
        return 99
    if report_key in PRIORITY_1_KEYS:
        return 1
    if report_key in PRIORITY_2_KEYS:
        return 2
    if report_key in PRIORITY_3_KEYS:
        return 3
    return 4


def determine_next_action(priority: int) -> str:
    """Translate numeric priority into a plain-English next step."""
    if priority == 1:
        return "Normalize first"
    if priority == 2:
        return "Normalize after core bank metrics"
    if priority == 3:
        return "Normalize after priority 1 and 2"
    if priority == 4:
        return "Review later"
    return "Classify manually or backlog"


def normalize_bool_flag(value: object) -> bool:
    """Safely convert mixed booleans/ints/strings into True/False."""
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_CSV}. Run analyze_raw_excel_report_matching.py first."
        )

    df = pd.read_csv(INPUT_CSV)

    if "report_key" not in df.columns:
        raise ValueError("Input CSV must include a 'report_key' column.")

    # Keep report_key consistent for grouping.
    df["report_key"] = df["report_key"].fillna("unknown").astype(str)

    # Aggregate into one row per report_key.
    rows = []
    for report_key, group in df.groupby("report_key", dropna=False):
        file_count = int(group["file_count"].sum()) if "file_count" in group.columns else int(len(group))

        first_period = group["first_period"].min() if "first_period" in group.columns else ""
        last_period = group["last_period"].max() if "last_period" in group.columns else ""

        if "period_count" in group.columns:
            period_count = int(group["period_count"].sum())
        elif "period_folder" in group.columns:
            period_count = int(group["period_folder"].nunique())
        else:
            period_count = 0

        if "sheet_count_total" in group.columns:
            sheet_count_total = int(group["sheet_count_total"].sum())
        elif "sheet_count" in group.columns:
            sheet_count_total = int(group["sheet_count"].sum())
        else:
            sheet_count_total = 0

        if "high_confidence_rows" in group.columns:
            high_confidence_rows = int(group["high_confidence_rows"].sum())
        elif "match_confidence" in group.columns:
            high_confidence_rows = int((group["match_confidence"] == "high").sum())
        else:
            high_confidence_rows = 0

        if "medium_confidence_rows" in group.columns:
            medium_confidence_rows = int(group["medium_confidence_rows"].sum())
        elif "match_confidence" in group.columns:
            medium_confidence_rows = int((group["match_confidence"] == "medium").sum())
        else:
            medium_confidence_rows = 0

        if "unmatched_rows" in group.columns:
            unmatched_rows = int(group["unmatched_rows"].sum())
        elif "match_confidence" in group.columns:
            unmatched_rows = int((group["match_confidence"] == "unmatched").sum())
        else:
            unmatched_rows = 0

        if "has_html_fallback" in group.columns:
            has_html_fallback = bool(group["has_html_fallback"].apply(normalize_bool_flag).any())
        elif "is_html_fallback" in group.columns:
            has_html_fallback = bool(group["is_html_fallback"].apply(normalize_bool_flag).any())
        elif "html_fallback_file_count" in group.columns:
            has_html_fallback = bool((group["html_fallback_file_count"] > 0).any())
        else:
            has_html_fallback = False

        priority = determine_priority(report_key)
        next_action = determine_next_action(priority)

        rows.append(
            {
                "report_key": report_key,
                "file_count": file_count,
                "first_period": first_period,
                "last_period": last_period,
                "period_count": period_count,
                "sheet_count_total": sheet_count_total,
                "high_confidence_rows": high_confidence_rows,
                "medium_confidence_rows": medium_confidence_rows,
                "unmatched_rows": unmatched_rows,
                "has_html_fallback": has_html_fallback,
                "dashboard_priority": priority,
                "suggested_next_action": next_action,
            }
        )

    matrix_df = pd.DataFrame(rows).sort_values(["dashboard_priority", "report_key"]).reset_index(drop=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    matrix_df.to_csv(OUTPUT_CSV, index=False)

    # Concise run summary requested for quick verification.
    total_report_keys = int(matrix_df["report_key"].nunique())
    priority_1_reports = int((matrix_df["dashboard_priority"] == 1).sum())
    priority_2_reports = int((matrix_df["dashboard_priority"] == 2).sum())
    unknown_reports = int((matrix_df["report_key"] == "unknown").sum())

    print(f"Report keys: {total_report_keys}")
    print(f"Priority 1 reports: {priority_1_reports}")
    print(f"Priority 2 reports: {priority_2_reports}")
    print(f"Unknown reports: {unknown_reports}")
    print(f"Output CSV: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
