"""Build a simple coverage matrix for CBU report types.

This script reads report-matching output and produces one row per report_key with
coverage and prioritization metadata for normalization planning.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

SUMMARY_CSV = Path("data/processed/raw_report_matching_summary.csv")
AUDIT_CSV = Path("data/processed/raw_report_matching_audit.csv")
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
    if not SUMMARY_CSV.exists():
        raise FileNotFoundError(
            f"Input file not found: {SUMMARY_CSV}. Run analyze_raw_excel_report_matching.py first."
        )
    if not AUDIT_CSV.exists():
        raise FileNotFoundError(
            f"Input file not found: {AUDIT_CSV}. Run analyze_raw_excel_report_matching.py first."
        )

    summary_df = pd.read_csv(SUMMARY_CSV)
    audit_df = pd.read_csv(AUDIT_CSV)

    if "report_key" not in summary_df.columns:
        raise ValueError("Summary CSV must include a 'report_key' column.")
    if "report_key" not in audit_df.columns:
        raise ValueError("Audit CSV must include a 'report_key' column.")

    # Keep report_key consistent for grouping.
    summary_df["report_key"] = summary_df["report_key"].fillna("unknown").astype(str)
    audit_df["report_key"] = audit_df["report_key"].fillna("unknown").astype(str)

    # Build a simple lookup table for confidence counts from the audit file.
    # We aggregate first so that duplicate report keys still become one final row.
    audit_totals = (
        audit_df.groupby("report_key", dropna=False)
        .agg(
            high_confidence_rows=("high_confidence_rows", "sum"),
            medium_confidence_rows=("medium_confidence_rows", "sum"),
            unmatched_rows=("unmatched_rows", "sum"),
        )
        .fillna(0)
        .astype(int)
    )

    # Aggregate summary data into one row per report_key.
    rows = []
    for report_key, group in summary_df.groupby("report_key", dropna=False):
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

        # Confidence totals must come from the audit file (not the summary file).
        if report_key in audit_totals.index:
            high_confidence_rows = int(audit_totals.loc[report_key, "high_confidence_rows"])
            medium_confidence_rows = int(audit_totals.loc[report_key, "medium_confidence_rows"])
            unmatched_rows = int(audit_totals.loc[report_key, "unmatched_rows"])
        else:
            high_confidence_rows = 0
            medium_confidence_rows = 0
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
