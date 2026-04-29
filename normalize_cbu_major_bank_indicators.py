"""Parse CBU major bank indicators Excel files into master + QA CSV outputs.

Run:
    python normalize_cbu_major_bank_indicators.py
    python normalize_cbu_major_bank_indicators.py --overwrite
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from openpyxl import load_workbook


RAW_ROOT = Path("data/raw/cbu_bankstats")
MASTER_OUTPUT = Path("data/master/cbu_major_bank_indicators_master.csv")
QA_OUTPUT = Path("data/master/cbu_major_bank_indicators_parse_qa.csv")
FILE_NAME_TOKEN = "Information-on-major-indicators-of-commercial-banks"


@dataclass
class ParsedRow:
    period_year: int
    period_month: int
    period: str
    bank_group: str
    bank_name: str
    indicator: str
    metric_type: str
    value: float
    unit: str
    source_file: str
    source_sheet: str
    source_cell: str
    loaded_at: str


@dataclass
class QAEntry:
    source_file: str
    period: str
    rows_read: int
    output_rows_created: int
    status: str
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse CBU major indicator Excel reports.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files if they already exist.",
    )
    return parser.parse_args()


def find_input_files(root_dir: Path) -> list[Path]:
    matching: list[Path] = []
    for path in root_dir.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            continue
        if FILE_NAME_TOKEN.lower() in path.name.lower():
            matching.append(path)
    return sorted(matching)


def find_period_from_path(file_path: Path) -> tuple[Optional[int], Optional[int], Optional[str]]:
    pattern = re.compile(r"^(\d{4})_(\d{2})$")
    for parent in file_path.parents:
        match = pattern.match(parent.name)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                return year, month, f"{year:04d}-{month:02d}"
    return None, None, None


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return " ".join(text.split())


def parse_number(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = clean_text(value)
    if not text:
        return None

    text = text.replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def classify_row(label: str) -> tuple[str, str]:
    lowered = label.lower()
    # Beginner-friendly heuristics:
    # - all-caps or words like "group" often indicate a group heading
    # - otherwise treat it as bank-level when possible
    if not label:
        return "needs_review", "needs_review"
    if label.isupper() or "group" in lowered or "state-owned" in lowered:
        return label, ""
    return "", label


def parse_workbook(file_path: Path, loaded_at: str) -> tuple[list[ParsedRow], QAEntry]:
    year, month, period = find_period_from_path(file_path)
    if year is None or month is None or period is None:
        qa = QAEntry(
            source_file=str(file_path),
            period="unknown",
            rows_read=0,
            output_rows_created=0,
            status="failed",
            notes="Could not infer period from folder path (expected YYYY_MM).",
        )
        return [], qa

    workbook = load_workbook(filename=file_path, data_only=True, read_only=True)
    sheet = workbook.active if workbook.active is not None else workbook.worksheets[0]

    output_rows: list[ParsedRow] = []
    rows_read = 0

    # Column mapping required by the business interpretation.
    column_map = {
        "C": ("assets", "absolute", "billion UZS"),
        "D": ("assets", "share", "percent"),
        "E": ("loans", "absolute", "billion UZS"),
        "F": ("loans", "share", "percent"),
        "G": ("capital", "absolute", "billion UZS"),
        "H": ("capital", "share", "percent"),
        "I": ("deposits", "absolute", "billion UZS"),
        "J": ("deposits", "share", "percent"),
    }

    for row_idx in range(1, sheet.max_row + 1):
        label = clean_text(sheet[f"B{row_idx}"].value)
        if not label:
            continue

        # Skip obvious header rows.
        lower_label = label.lower()
        if any(token in lower_label for token in ["bank", "assets", "loans", "capital", "deposits", "share"]):
            if row_idx <= 20:
                continue

        bank_group, bank_name = classify_row(label)

        row_created = 0
        for col_letter, (indicator, metric_type, unit) in column_map.items():
            cell_ref = f"{col_letter}{row_idx}"
            number = parse_number(sheet[cell_ref].value)
            if number is None:
                continue

            output_rows.append(
                ParsedRow(
                    period_year=year,
                    period_month=month,
                    period=period,
                    bank_group=bank_group if bank_group else "",
                    bank_name=bank_name if bank_name else "",
                    indicator=indicator,
                    metric_type=metric_type,
                    value=number,
                    unit=unit,
                    source_file=str(file_path),
                    source_sheet=sheet.title,
                    source_cell=cell_ref,
                    loaded_at=loaded_at,
                )
            )
            row_created += 1

        if row_created > 0:
            rows_read += 1

    status = "ok" if output_rows else "warning"
    notes = "Parsed successfully." if output_rows else "No numeric indicator values found in C:J."

    qa = QAEntry(
        source_file=str(file_path),
        period=period,
        rows_read=rows_read,
        output_rows_created=len(output_rows),
        status=status,
        notes=notes,
    )
    return output_rows, qa


def write_master(rows: list[ParsedRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "period_year",
                "period_month",
                "period",
                "bank_group",
                "bank_name",
                "indicator",
                "metric_type",
                "value",
                "unit",
                "source_file",
                "source_sheet",
                "source_cell",
                "loaded_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.period_year,
                    row.period_month,
                    row.period,
                    row.bank_group,
                    row.bank_name,
                    row.indicator,
                    row.metric_type,
                    row.value,
                    row.unit,
                    row.source_file,
                    row.source_sheet,
                    row.source_cell,
                    row.loaded_at,
                ]
            )


def write_qa(rows: list[QAEntry], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_file", "period", "rows_read", "output_rows_created", "status", "notes"])
        for row in rows:
            writer.writerow([row.source_file, row.period, row.rows_read, row.output_rows_created, row.status, row.notes])


def main() -> None:
    args = parse_args()

    if (MASTER_OUTPUT.exists() or QA_OUTPUT.exists()) and not args.overwrite:
        raise SystemExit(
            "Output file already exists. Re-run with --overwrite to replace: "
            f"{MASTER_OUTPUT} and/or {QA_OUTPUT}"
        )

    if not RAW_ROOT.exists():
        raise SystemExit(f"Input directory does not exist: {RAW_ROOT}")

    files = find_input_files(RAW_ROOT)
    if not files:
        raise SystemExit(
            "No matching Excel files found under data/raw/cbu_bankstats/ with filename containing "
            f"'{FILE_NAME_TOKEN}'."
        )

    loaded_at = datetime.now(timezone.utc).isoformat()

    master_rows: list[ParsedRow] = []
    qa_rows: list[QAEntry] = []

    for file_path in files:
        parsed_rows, qa_entry = parse_workbook(file_path=file_path, loaded_at=loaded_at)
        master_rows.extend(parsed_rows)
        qa_rows.append(qa_entry)

    write_master(master_rows, MASTER_OUTPUT)
    write_qa(qa_rows, QA_OUTPUT)

    print(f"Parsed {len(files)} file(s).")
    print(f"Master rows written: {len(master_rows)} -> {MASTER_OUTPUT}")
    print(f"QA rows written: {len(qa_rows)} -> {QA_OUTPUT}")


if __name__ == "__main__":
    main()
