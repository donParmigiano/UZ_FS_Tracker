"""Collector test for April 2026 CBU banking statistics reports.

Workflow:
1) Crawl the April 2026 listing page.
2) Visit each report page and extract Excel links.
3) Download Excel files to data/raw/cbu_bankstats/2026_04/.
4) Create/refresh cbu_bankstats_sources table in SQLite.
5) Export inventory CSV and parse each sheet to CSV.
6) Export parse summary CSV.
"""

from __future__ import annotations

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


def parse_excel_to_csv(local_file: Path) -> tuple[int, int]:
    excel = pd.ExcelFile(local_file)
    rows_total = 0
    sheets_total = 0
    for sheet in excel.sheet_names:
        df = excel.parse(sheet_name=sheet)
        rows_total += len(df)
        sheets_total += 1
        sheet_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", sheet.strip() or "sheet")[:80]
        out_name = f"{local_file.stem}__{sheet_slug}.csv"
        out_path = PROCESSED_DIR / out_name
        df.to_csv(out_path, index=False)
    return rows_total, sheets_total


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
    downloaded = sum(r.download_status == "downloaded" for r in records)
    parsed = sum(r.parse_status == "parsed" for r in records)
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


def run() -> None:
    ensure_dirs()
    listing_html = fetch_text(LISTING_URL)
    report_links = extract_report_links(listing_html)

    records: list[SourceRecord] = []
    for report_url in report_links:
        try:
            report_html = fetch_text(report_url)
            file_links = extract_excel_links(report_html, report_url)
        except Exception as exc:
            records.append(
                SourceRecord(
                    listing_url=LISTING_URL,
                    report_url=report_url,
                    file_url="",
                    local_path="",
                    download_status="failed",
                    parse_status="failed",
                    error_message=f"report_open_error: {exc}",
                )
            )
            continue

        for file_url in file_links:
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
                payload = fetch_bytes(file_url)
                local_file.write_bytes(payload)
                rec.download_status = "downloaded"
            except Exception as exc:
                rec.download_status = "failed"
                rec.parse_status = "failed"
                rec.error_message = f"download_error: {exc}"
                records.append(rec)
                continue

            try:
                rows_total, sheets_total = parse_excel_to_csv(local_file)
                rec.parse_status = "parsed"
                rec.rows_total = rows_total
                rec.sheets_total = sheets_total
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
    print(f"Done: {len(records)} source records processed.")


if __name__ == "__main__":
    run()
