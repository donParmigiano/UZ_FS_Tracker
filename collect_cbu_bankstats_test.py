"""Collector test for April 2026 CBU banking statistics reports.

This script can:
1) Crawl the April 2026 listing page and discover report pages.
2) Visit each report page and discover Excel links.
3) Download Excel files to data/raw/cbu_bankstats/2026_04/.
4) Parse each Excel sheet into CSV files in data/processed/cbu_bankstats/2026_04/.
5) Export source inventory, parse summary, and parse QA reports.

Important:
- Raw Excel files are kept as the source of truth.
- CSV files are convenience outputs for review.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd

from src.common.constants import DB_PATH

LISTING_URL = (
    "https://cbu.uz/en/statistics/bankstats/"
    "?arFilter_DATE_ACTIVE_FROM_1=&arFilter_DATE_ACTIVE_FROM_2="
    "&arFilter_ff%5BSECTION_ID%5D=3497&year=2026&month=04&set_filter=&set_filter=Y"
)
BASE_URL = "https://cbu.uz"

RAW_DIR = Path("data/raw/cbu_bankstats/2026_04")
PROCESSED_DIR = Path("data/processed/cbu_bankstats/2026_04")
INVENTORY_CSV = Path("data/processed/cbu_bankstats_inventory_2026_04.csv")
PARSE_SUMMARY_CSV = Path("data/processed/cbu_bankstats_parse_summary_2026_04.csv")
PARSE_QA_CSV = Path("data/processed/cbu_bankstats_parse_qa_2026_04.csv")

REPORT_LINK_PATTERN = re.compile(r'href=["\']([^"\']+/statistics/bankstats/[^"\']+)["\']', re.IGNORECASE)
FILE_LINK_PATTERN = re.compile(r'href=["\']([^"\']+\.(?:xlsx|xls))(?:\?[^"\']*)?["\']', re.IGNORECASE)


@dataclass
class SourceRecord:
    listing_url: str
    report_url: str
    file_url: str
    local_path: str
    download_status: str
    parse_status: str
    error_message: str
    rows_total: int = 0
    sheets_total: int = 0


@dataclass
class QARecord:
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


def fetch_text(url: str, timeout: int = 45) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Collector-Test/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def fetch_bytes(url: str, timeout: int = 90) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Collector-Test/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def unique_ordered(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def extract_report_links(listing_html: str) -> list[str]:
    links = [urljoin(BASE_URL, m.group(1)) for m in REPORT_LINK_PATTERN.finditer(listing_html)]
    return unique_ordered(links)


def extract_excel_links(report_html: str, report_url: str) -> list[str]:
    links = [urljoin(report_url, m.group(1)) for m in FILE_LINK_PATTERN.finditer(report_html)]
    return unique_ordered(links)


def safe_stem_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "download"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if len(stem) > 140:
        stem = stem[:140]
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
    return f"{Path(stem).stem}_{digest}{Path(stem).suffix or '.xlsx'}"


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    INVENTORY_CSV.parent.mkdir(parents=True, exist_ok=True)


def create_sources_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cbu_bankstats_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_url TEXT NOT NULL,
            report_url TEXT NOT NULL,
            file_url TEXT NOT NULL,
            local_path TEXT,
            download_status TEXT,
            parse_status TEXT,
            error_message TEXT,
            rows_total INTEGER DEFAULT 0,
            sheets_total INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def reset_sources_table(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM cbu_bankstats_sources")
    conn.commit()


def insert_sources(conn: sqlite3.Connection, records: list[SourceRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO cbu_bankstats_sources (
            listing_url, report_url, file_url, local_path,
            download_status, parse_status, error_message, rows_total, sheets_total
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.listing_url,
                r.report_url,
                r.file_url,
                r.local_path,
                r.download_status,
                r.parse_status,
                r.error_message,
                r.rows_total,
                r.sheets_total,
            )
            for r in records
        ],
    )
    conn.commit()


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Preserve data values for export while keeping text columns untouched."""
    normalized = df.copy()
    numeric_columns = normalized.select_dtypes(include=["number"]).columns
    if len(numeric_columns) == 0:
        return normalized

    # Keep original numeric precision from pandas parse; only clean negative zero display.
    normalized[numeric_columns] = normalized[numeric_columns].where(normalized[numeric_columns] != 0, 0)
    return normalized


def sheet_output_path(local_file: Path, sheet_name: str) -> Path:
    sheet_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", sheet_name.strip() or "sheet")[:80]
    return PROCESSED_DIR / f"{local_file.stem}__{sheet_slug}.csv"


def parse_excel_to_csv(local_file: Path, overwrite: bool = False, decimals: int = 12) -> tuple[int, int, list[QARecord]]:
    excel = pd.ExcelFile(local_file)
    rows_total = 0
    sheets_total = 0
    qa_records: list[QARecord] = []

    for sheet in excel.sheet_names:
        df = excel.parse(sheet_name=sheet)
        excel_rows = int(df.shape[0])
        excel_cols = int(df.shape[1])
        numeric_cells = int(df.select_dtypes(include=["number"]).count().sum())
        blank_cells = int(df.isna().sum().sum())

        out_path = sheet_output_path(local_file, sheet)
        normalized_df = normalize_numeric_columns(df)
        csv_existed_before = out_path.exists()

        if csv_existed_before and not overwrite:
            print(f"[parse] skipped existing file: {out_path}")
            parse_status = "skipped_existing"
            notes = "CSV exists and overwrite=False"
            csv_rows = excel_rows
            csv_cols = excel_cols
        else:
            normalized_df.to_csv(out_path, index=False, float_format=f"%.{decimals}f")
            if csv_existed_before and overwrite:
                print(f"[parse] overwritten file: {out_path}")
            else:
                print(f"[parse] created new file: {out_path}")
            parse_status = "written"
            notes = f"numeric columns exported with up to {decimals} decimal places"
            csv_rows = int(normalized_df.shape[0])
            csv_cols = int(normalized_df.shape[1])

        rows_total += excel_rows
        sheets_total += 1
        qa_records.append(
            QARecord(
                source_excel_file=str(local_file),
                sheet_name=sheet,
                parsed_csv_file=str(out_path),
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

    return rows_total, sheets_total, qa_records


def export_inventory(records: list[SourceRecord]) -> None:
    with INVENTORY_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "listing_url",
                "report_url",
                "file_url",
                "local_path",
                "download_status",
                "parse_status",
                "rows_total",
                "sheets_total",
                "error_message",
            ],
        )
        writer.writeheader()
        for r in records:
            writer.writerow(
                {
                    "listing_url": r.listing_url,
                    "report_url": r.report_url,
                    "file_url": r.file_url,
                    "local_path": r.local_path,
                    "download_status": r.download_status,
                    "parse_status": r.parse_status,
                    "rows_total": r.rows_total,
                    "sheets_total": r.sheets_total,
                    "error_message": r.error_message,
                }
            )


def export_parse_summary(records: list[SourceRecord]) -> None:
    total_files = len(records)
    downloaded = sum(r.download_status in {"downloaded", "skipped_existing", "existing_raw"} for r in records)
    parsed = sum(r.parse_status in {"parsed", "parsed_with_skips"} for r in records)
    total_rows = sum(r.rows_total for r in records)
    total_sheets = sum(r.sheets_total for r in records)

    summary = pd.DataFrame(
        [
            {
                "month": "2026_04",
                "files_found": total_files,
                "files_downloaded": downloaded,
                "files_parsed": parsed,
                "sheets_parsed": total_sheets,
                "rows_parsed": total_rows,
            }
        ]
    )
    summary.to_csv(PARSE_SUMMARY_CSV, index=False)


def export_parse_qa(qa_records: list[QARecord]) -> None:
    with PARSE_QA_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
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
        for r in qa_records:
            writer.writerow(r.__dict__)


def discover_remote_files() -> list[tuple[str, str]]:
    """Return list of (report_url, file_url) discovered from the listing page."""
    listing_html = fetch_text(LISTING_URL)
    report_links = extract_report_links(listing_html)
    discovered: list[tuple[str, str]] = []

    for report_url in report_links:
        report_html = fetch_text(report_url)
        for file_url in extract_excel_links(report_html, report_url):
            discovered.append((report_url, file_url))

    return discovered


def run(overwrite: bool = False, parse_only: bool = False) -> None:
    ensure_dirs()
    records: list[SourceRecord] = []
    qa_records: list[QARecord] = []

    if parse_only:
        print("[mode] parse-only enabled: skipping downloads and using existing raw Excel files.")
        raw_files = sorted([p for p in RAW_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"}])

        for local_file in raw_files:
            rec = SourceRecord(
                listing_url=LISTING_URL,
                report_url="",
                file_url="",
                local_path=str(local_file),
                download_status="existing_raw",
                parse_status="pending",
                error_message="",
            )
            try:
                rows_total, sheets_total, qa = parse_excel_to_csv(local_file, overwrite=overwrite, decimals=12)
                rec.rows_total = rows_total
                rec.sheets_total = sheets_total
                rec.parse_status = "parsed_with_skips" if any(q.parse_status == "skipped_existing" for q in qa) else "parsed"
                qa_records.extend(qa)
            except Exception as exc:
                rec.parse_status = "failed"
                rec.error_message = f"parse_error: {exc}"
            records.append(rec)
    else:
        discovered = discover_remote_files()
        for report_url, file_url in discovered:
            local_file = RAW_DIR / safe_stem_from_url(file_url)
            rec = SourceRecord(
                listing_url=LISTING_URL,
                report_url=report_url,
                file_url=file_url,
                local_path=str(local_file),
                download_status="pending",
                parse_status="pending",
                error_message="",
            )

            try:
                raw_existed_before = local_file.exists()
                if raw_existed_before and not overwrite:
                    print(f"[download] skipped existing file: {local_file}")
                    rec.download_status = "skipped_existing"
                else:
                    payload = fetch_bytes(file_url)
                    local_file.write_bytes(payload)
                    if raw_existed_before and overwrite:
                        print(f"[download] overwritten file: {local_file}")
                    else:
                        print(f"[download] created new file: {local_file}")
                    rec.download_status = "downloaded"
            except Exception as exc:
                rec.download_status = "failed"
                rec.parse_status = "failed"
                rec.error_message = f"download_error: {exc}"
                records.append(rec)
                continue

            try:
                rows_total, sheets_total, qa = parse_excel_to_csv(local_file, overwrite=overwrite, decimals=12)
                rec.rows_total = rows_total
                rec.sheets_total = sheets_total
                rec.parse_status = "parsed_with_skips" if any(q.parse_status == "skipped_existing" for q in qa) else "parsed"
                qa_records.extend(qa)
            except Exception as exc:
                rec.parse_status = "failed"
                rec.error_message = f"parse_error: {exc}"

            records.append(rec)

    with sqlite3.connect(DB_PATH) as conn:
        create_sources_table(conn)
        reset_sources_table(conn)
        insert_sources(conn, records)

    export_inventory(records)
    export_parse_summary(records)
    export_parse_qa(qa_records)
    print(f"Done: {len(records)} source records processed. QA rows: {len(qa_records)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and parse April 2026 CBU bankstats files.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing raw and processed files.",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip downloads and parse existing raw Excel files only.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(overwrite=args.overwrite, parse_only=args.parse_only)
