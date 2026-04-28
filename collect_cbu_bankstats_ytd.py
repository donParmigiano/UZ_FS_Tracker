"""Collect and parse CBU banking statistics for 2026 year-to-date (Jan-Apr).

Layers:
- raw: downloaded Excel files (source of truth)
- processed: parsed CSVs + inventory/summary/QA
- master: technical cell-level master for cross-month inspection
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd

BASE_URL = "https://cbu.uz/en/statistics/bankstats/"
SECTION_ID = "3497"
TARGET_MONTHS = ["2026_01", "2026_02", "2026_03", "2026_04"]

RAW_BASE_DIR = Path("data/raw/cbu_bankstats")
PROCESSED_BASE_DIR = Path("data/processed/cbu_bankstats")
MASTER_DIR = Path("data/master")

INVENTORY_CSV = Path("data/processed/cbu_bankstats_inventory_2026_ytd.csv")
SUMMARY_CSV = Path("data/processed/cbu_bankstats_parse_summary_2026_ytd.csv")
QA_CSV = Path("data/processed/cbu_bankstats_parse_qa_2026_ytd.csv")
MASTER_CSV = Path("data/master/cbu_bankstats_cells_master_2026_ytd.csv")

REPORT_LINK_PATTERN = re.compile(r'href=["\']([^"\']+/statistics/bankstats/[^"\']+)["\']', re.IGNORECASE)
FILE_LINK_PATTERN = re.compile(r'href=["\']([^"\']+\.(?:xlsx|xls))(?:\?[^"\']*)?["\']', re.IGNORECASE)
H1_PATTERN = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
TAG_PATTERN = re.compile(r"<[^>]+>")


@dataclass
class SourceRecord:
    period_year: int
    period_month: int
    period: str
    report_title: str
    listing_url: str
    report_url: str
    file_url: str
    source_excel_file: str
    download_status: str
    parse_status: str
    sheets_total: int
    rows_total: int
    parsed_csv_files: int
    error_message: str


@dataclass
class QARecord:
    period_year: int
    period_month: int
    period: str
    report_title: str
    source_excel_file: str
    sheet_name: str
    parsed_csv_file: str
    excel_rows_detected: int
    excel_columns_detected: int
    csv_rows_written: int
    csv_columns_written: int
    numeric_cells_detected: int
    blank_cells_detected: int
    parse_status: str
    notes: str


def month_to_listing_url(period: str) -> str:
    year, month = period.split("_")
    return (
        f"{BASE_URL}?arFilter_DATE_ACTIVE_FROM_1=&arFilter_DATE_ACTIVE_FROM_2="
        f"&arFilter_ff%5BSECTION_ID%5D={SECTION_ID}&year={year}&month={month}&set_filter=&set_filter=Y"
    )


def fetch_text(url: str, timeout: int = 45) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Bankstats-YTD/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def fetch_bytes(url: str, timeout: int = 90) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Bankstats-YTD/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def unique_ordered(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def extract_report_links(listing_html: str) -> list[str]:
    links = [urljoin(BASE_URL, m.group(1)) for m in REPORT_LINK_PATTERN.finditer(listing_html)]
    return unique_ordered(links)


def extract_excel_links(report_html: str, report_url: str) -> list[str]:
    links = [urljoin(report_url, m.group(1)) for m in FILE_LINK_PATTERN.finditer(report_html)]
    return unique_ordered(links)


def extract_report_title(report_html: str, report_url: str) -> str:
    match = H1_PATTERN.search(report_html)
    if match:
        text = TAG_PATTERN.sub(" ", match.group(1))
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text
    path_name = Path(urlparse(report_url).path).name
    return path_name or report_url


def safe_file_name_from_url(file_url: str) -> str:
    parsed = urlparse(file_url)
    base_name = Path(parsed.path).name or "download.xlsx"
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name)
    digest = hashlib.md5(file_url.encode("utf-8")).hexdigest()[:8]
    suffix = Path(clean).suffix or ".xlsx"
    stem = Path(clean).stem
    return f"{stem}_{digest}{suffix}"


def sheet_output_path(period: str, local_excel: Path, sheet_name: str) -> Path:
    sheet_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", sheet_name.strip() or "sheet")[:80]
    return PROCESSED_BASE_DIR / period / f"{local_excel.stem}__{sheet_slug}.csv"


def ensure_dirs() -> None:
    RAW_BASE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_BASE_DIR.mkdir(parents=True, exist_ok=True)
    INVENTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    MASTER_DIR.mkdir(parents=True, exist_ok=True)


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    numeric_columns = out.select_dtypes(include=["number"]).columns
    if len(numeric_columns) > 0:
        out[numeric_columns] = out[numeric_columns].where(out[numeric_columns] != 0, 0)
    return out


def parse_excel_to_csv(period: str, source_excel_file: Path, overwrite: bool, decimals: int = 12) -> tuple[int, int, int, list[QARecord], list[Path]]:
    excel = pd.ExcelFile(source_excel_file)
    rows_total = 0
    sheets_total = 0
    parsed_csv_count = 0
    qa_rows: list[QARecord] = []
    parsed_csv_paths: list[Path] = []

    for sheet_name in excel.sheet_names:
        df = excel.parse(sheet_name=sheet_name)
        output_csv = sheet_output_path(period, source_excel_file, sheet_name)
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        excel_rows = int(df.shape[0])
        excel_cols = int(df.shape[1])
        numeric_cells = int(df.select_dtypes(include=["number"]).count().sum())
        blank_cells = int(df.isna().sum().sum())

        normalized_df = normalize_numeric_columns(df)
        existed_before = output_csv.exists()
        if existed_before and not overwrite:
            print(f"[parse] skipped existing file: {output_csv}")
            parse_status = "skipped_existing"
            notes = "CSV exists and overwrite=False"
            csv_rows = excel_rows
            csv_cols = excel_cols
        else:
            normalized_df.to_csv(output_csv, index=False, float_format=f"%.{decimals}f")
            if existed_before:
                print(f"[parse] overwritten file: {output_csv}")
            else:
                print(f"[parse] created new file: {output_csv}")
            parse_status = "written"
            notes = f"numeric columns exported with up to {decimals} decimal places"
            csv_rows = int(normalized_df.shape[0])
            csv_cols = int(normalized_df.shape[1])

        rows_total += excel_rows
        sheets_total += 1
        parsed_csv_count += 1
        parsed_csv_paths.append(output_csv)

        qa_rows.append(
            QARecord(
                period_year=int(period.split("_")[0]),
                period_month=int(period.split("_")[1]),
                period=period,
                report_title="",
                source_excel_file=str(source_excel_file),
                sheet_name=sheet_name,
                parsed_csv_file=str(output_csv),
                excel_rows_detected=excel_rows,
                excel_columns_detected=excel_cols,
                csv_rows_written=csv_rows,
                csv_columns_written=csv_cols,
                numeric_cells_detected=numeric_cells,
                blank_cells_detected=blank_cells,
                parse_status=parse_status,
                notes=notes,
            )
        )

    return rows_total, sheets_total, parsed_csv_count, qa_rows, parsed_csv_paths


def export_inventory(records: list[SourceRecord]) -> None:
    with INVENTORY_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "period_year",
                "period_month",
                "period",
                "report_title",
                "listing_url",
                "report_url",
                "file_url",
                "source_excel_file",
                "download_status",
                "parse_status",
                "sheets_total",
                "rows_total",
                "parsed_csv_files",
                "error_message",
            ],
        )
        writer.writeheader()
        for r in records:
            writer.writerow(r.__dict__)


def export_parse_summary(records: list[SourceRecord]) -> None:
    if not records:
        pd.DataFrame(
            [
                {
                    "period": "2026_ytd",
                    "months_processed": 0,
                    "reports_found": 0,
                    "excel_files_discovered": 0,
                    "excel_files_downloaded_or_existing": 0,
                    "excel_download_failures": 0,
                    "parsed_csv_files": 0,
                    "sheets_parsed": 0,
                    "rows_parsed": 0,
                }
            ]
        ).to_csv(SUMMARY_CSV, index=False)
        return

    months_processed = len({r.period for r in records})
    reports_found = len({(r.period, r.report_url) for r in records if r.report_url})
    excel_discovered = len(records)
    downloaded_or_existing = sum(r.download_status in {"downloaded", "skipped_existing", "existing_raw"} for r in records)
    download_failures = sum(r.download_status == "failed" for r in records)
    parsed_csv_files = sum(r.parsed_csv_files for r in records)
    sheets_parsed = sum(r.sheets_total for r in records)
    rows_parsed = sum(r.rows_total for r in records)

    summary_df = pd.DataFrame(
        [
            {
                "period": "2026_ytd",
                "months_processed": months_processed,
                "reports_found": reports_found,
                "excel_files_discovered": excel_discovered,
                "excel_files_downloaded_or_existing": downloaded_or_existing,
                "excel_download_failures": download_failures,
                "parsed_csv_files": parsed_csv_files,
                "sheets_parsed": sheets_parsed,
                "rows_parsed": rows_parsed,
            }
        ]
    )
    summary_df.to_csv(SUMMARY_CSV, index=False)


def export_parse_qa(qa_records: list[QARecord]) -> None:
    with QA_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "period_year",
                "period_month",
                "period",
                "report_title",
                "source_excel_file",
                "sheet_name",
                "parsed_csv_file",
                "excel_rows_detected",
                "excel_columns_detected",
                "csv_rows_written",
                "csv_columns_written",
                "numeric_cells_detected",
                "blank_cells_detected",
                "parse_status",
                "notes",
            ],
        )
        writer.writeheader()
        for row in qa_records:
            writer.writerow(row.__dict__)


def build_cells_master(records: list[SourceRecord], qa_records: list[QARecord]) -> int:
    """Create auditable cell-level master CSV from parsed sheet CSVs."""
    lookup_by_csv = {q.parsed_csv_file: q for q in qa_records}
    loaded_at = datetime.now(timezone.utc).isoformat()
    master_rows: list[dict[str, object]] = []

    for record in records:
        if record.parse_status not in {"parsed", "parsed_with_skips"}:
            continue

        csv_files = sorted((PROCESSED_BASE_DIR / record.period).glob(f"{Path(record.source_excel_file).stem}__*.csv"))
        for csv_file in csv_files:
            qa = lookup_by_csv.get(str(csv_file))
            sheet_name = qa.sheet_name if qa else csv_file.stem.split("__", 1)[-1]

            try:
                df = pd.read_csv(csv_file, dtype=object, keep_default_na=False)
            except Exception as exc:
                print(f"[master] skipped {csv_file}: {exc}")
                continue

            for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
                for col_idx, col_name in enumerate(df.columns, start=1):
                    master_rows.append(
                        {
                            "period_year": record.period_year,
                            "period_month": record.period_month,
                            "period": record.period,
                            "report_title": record.report_title,
                            "source_excel_file": record.source_excel_file,
                            "parsed_csv_file": str(csv_file),
                            "sheet_name": sheet_name,
                            "row_number": row_idx,
                            "column_number": col_idx,
                            "column_name": str(col_name),
                            "cell_value": row[col_name],
                            "source_url": record.file_url,
                            "loaded_at": loaded_at,
                        }
                    )

    master_df = pd.DataFrame(
        master_rows,
        columns=[
            "period_year",
            "period_month",
            "period",
            "report_title",
            "source_excel_file",
            "parsed_csv_file",
            "sheet_name",
            "row_number",
            "column_number",
            "column_name",
            "cell_value",
            "source_url",
            "loaded_at",
        ],
    )
    master_df.to_csv(MASTER_CSV, index=False)
    return len(master_df)


def run(overwrite: bool = False, parse_only: bool = False) -> None:
    ensure_dirs()
    source_records: list[SourceRecord] = []
    qa_records: list[QARecord] = []

    for period in TARGET_MONTHS:
        year = int(period.split("_")[0])
        month = int(period.split("_")[1])
        listing_url = month_to_listing_url(period)
        print(f"\n=== Processing {period} ===")

        if parse_only:
            print("[mode] parse-only enabled: skipping discovery/download for this month.")
            raw_month_dir = RAW_BASE_DIR / period
            raw_month_dir.mkdir(parents=True, exist_ok=True)
            excel_files = sorted([p for p in raw_month_dir.glob("*") if p.suffix.lower() in {".xlsx", ".xls"}])

            for excel_file in excel_files:
                report_title = excel_file.stem
                record = SourceRecord(
                    period_year=year,
                    period_month=month,
                    period=period,
                    report_title=report_title,
                    listing_url=listing_url,
                    report_url="",
                    file_url="",
                    source_excel_file=str(excel_file),
                    download_status="existing_raw",
                    parse_status="pending",
                    sheets_total=0,
                    rows_total=0,
                    parsed_csv_files=0,
                    error_message="",
                )
                try:
                    rows_total, sheets_total, parsed_csv_files, qa_rows, _ = parse_excel_to_csv(period, excel_file, overwrite=overwrite, decimals=12)
                    for qa in qa_rows:
                        qa.report_title = report_title
                    qa_records.extend(qa_rows)
                    record.rows_total = rows_total
                    record.sheets_total = sheets_total
                    record.parsed_csv_files = parsed_csv_files
                    record.parse_status = "parsed_with_skips" if any(q.parse_status == "skipped_existing" for q in qa_rows) else "parsed"
                except Exception as exc:
                    record.parse_status = "failed"
                    record.error_message = f"parse_error: {exc}"
                source_records.append(record)
            continue

        try:
            listing_html = fetch_text(listing_url)
            report_links = extract_report_links(listing_html)
            print(f"[discover] report pages found: {len(report_links)}")
        except Exception as exc:
            print(f"[discover] failed listing page for {period}: {exc}")
            continue

        for report_url in report_links:
            try:
                report_html = fetch_text(report_url)
                report_title = extract_report_title(report_html, report_url)
                file_links = extract_excel_links(report_html, report_url)
                print(f"[discover] {period} | {report_title} | files: {len(file_links)}")
            except Exception as exc:
                source_records.append(
                    SourceRecord(
                        period_year=year,
                        period_month=month,
                        period=period,
                        report_title="",
                        listing_url=listing_url,
                        report_url=report_url,
                        file_url="",
                        source_excel_file="",
                        download_status="failed",
                        parse_status="failed",
                        sheets_total=0,
                        rows_total=0,
                        parsed_csv_files=0,
                        error_message=f"report_open_error: {exc}",
                    )
                )
                continue

            for file_url in file_links:
                raw_dir = RAW_BASE_DIR / period
                raw_dir.mkdir(parents=True, exist_ok=True)
                local_excel = raw_dir / safe_file_name_from_url(file_url)
                record = SourceRecord(
                    period_year=year,
                    period_month=month,
                    period=period,
                    report_title=report_title,
                    listing_url=listing_url,
                    report_url=report_url,
                    file_url=file_url,
                    source_excel_file=str(local_excel),
                    download_status="pending",
                    parse_status="pending",
                    sheets_total=0,
                    rows_total=0,
                    parsed_csv_files=0,
                    error_message="",
                )

                try:
                    existed_before = local_excel.exists()
                    if existed_before and not overwrite:
                        print(f"[download] skipped existing file: {local_excel}")
                        record.download_status = "skipped_existing"
                    else:
                        payload = fetch_bytes(file_url)
                        local_excel.write_bytes(payload)
                        if existed_before:
                            print(f"[download] overwritten file: {local_excel}")
                        else:
                            print(f"[download] created new file: {local_excel}")
                        record.download_status = "downloaded"
                except Exception as exc:
                    record.download_status = "failed"
                    record.parse_status = "failed"
                    record.error_message = f"download_error: {exc}"
                    source_records.append(record)
                    continue

                try:
                    rows_total, sheets_total, parsed_csv_files, qa_rows, _ = parse_excel_to_csv(period, local_excel, overwrite=overwrite, decimals=12)
                    for qa in qa_rows:
                        qa.report_title = report_title
                    qa_records.extend(qa_rows)
                    record.rows_total = rows_total
                    record.sheets_total = sheets_total
                    record.parsed_csv_files = parsed_csv_files
                    record.parse_status = "parsed_with_skips" if any(q.parse_status == "skipped_existing" for q in qa_rows) else "parsed"
                except Exception as exc:
                    record.parse_status = "failed"
                    record.error_message = f"parse_error: {exc}"

                source_records.append(record)

    export_inventory(source_records)
    export_parse_summary(source_records)
    export_parse_qa(qa_records)
    master_row_count = build_cells_master(source_records, qa_records)

    print("\n=== YTD run complete ===")
    print(f"Inventory: {INVENTORY_CSV}")
    print(f"Parse summary: {SUMMARY_CSV}")
    print(f"Parse QA: {QA_CSV}")
    print(f"Master cells: {MASTER_CSV} (rows={master_row_count})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and parse CBU bankstats for 2026 YTD (Jan-Apr).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing raw and processed files.")
    parser.add_argument("--parse-only", action="store_true", help="Parse existing raw Excel files only.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(overwrite=args.overwrite, parse_only=args.parse_only)
