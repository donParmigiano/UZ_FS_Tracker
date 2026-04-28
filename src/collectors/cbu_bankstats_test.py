"""Central Bank of Uzbekistan bankstats collector for a single controlled test month (April 2026)."""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.common.constants import DB_PATH

BASE_URL = "https://cbu.uz"
TARGET_YEAR = 2026
TARGET_MONTH = 4
MONTHLY_URL = (
    "https://cbu.uz/en/statistics/bankstats/"
    "?arFilter_DATE_ACTIVE_FROM_1=&arFilter_DATE_ACTIVE_FROM_2="
    "&arFilter_ff%5BSECTION_ID%5D=3497&year=2026&month=04&set_filter=&set_filter=Y"
)
RAW_DIR = Path("data/raw/cbu_bankstats/2026_04")
PROCESSED_DIR = Path("data/processed/cbu_bankstats/2026_04")
INVENTORY_CSV = Path("data/processed/cbu_bankstats_inventory_2026_04.csv")
PARSE_SUMMARY_CSV = Path("data/processed/cbu_bankstats_parse_summary_2026_04.csv")
LOG_FILE = Path("logs/cbu_bankstats_test.log")

REQUEST_DELAY_SECONDS = 0.5
TIMEOUT_SECONDS = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UZ-FS-Tracker/0.2; +https://cbu.uz)",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class SourceRecord:
    source_id: str
    period_year: int
    period_month: int
    report_title: str
    report_page_url: str
    excel_file_url: str
    local_file_path: str
    file_type: str
    downloaded_at: str
    status: str
    notes: str


def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
    )


def safe_get(session: requests.Session, url: str) -> requests.Response | None:
    try:
        response = session.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response
    except requests.RequestException as exc:
        logging.error("Request failed for %s: %s", url, exc)
        return None


def extract_report_links(monthly_html: str) -> list[str]:
    soup = BeautifulSoup(monthly_html, "html.parser")
    links: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        absolute = urljoin(BASE_URL, href)
        if re.search(r"/en/statistics/bankstats/\d+/?$", absolute):
            links.add(absolute)
    return sorted(links)


def extract_excel_links(report_html: str) -> list[str]:
    soup = BeautifulSoup(report_html, "html.parser")
    links: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if ".xlsx" in href.lower() or ".xls" in href.lower():
            links.add(urljoin(BASE_URL, href))
    return sorted(links)


def get_report_title(report_html: str) -> str:
    soup = BeautifulSoup(report_html, "html.parser")
    if soup.title and soup.title.text.strip():
        return soup.title.text.strip()
    h1 = soup.find("h1")
    if h1 and h1.text.strip():
        return h1.text.strip()
    return "Untitled report"


def file_extension(file_url: str) -> str:
    cleaned = file_url.split("?")[0].lower()
    if cleaned.endswith(".xlsx"):
        return "xlsx"
    if cleaned.endswith(".xls"):
        return "xls"
    return "unknown"


def sanitize_filename(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")
    return sanitized or "cbu_bankstats_file"


def download_excel(session: requests.Session, file_url: str, report_id: str, index: int) -> tuple[str, str, str]:
    extension = file_extension(file_url)
    if extension == "unknown":
        return "", extension, "Skipped: unsupported file extension"

    filename = sanitize_filename(file_url.split("/")[-1].split("?")[0])
    if not filename.lower().endswith(f".{extension}"):
        filename = f"{filename}.{extension}"
    local_name = f"{report_id}_{index}_{filename}"
    local_path = RAW_DIR / local_name

    response = safe_get(session, file_url)
    if response is None:
        return str(local_path), extension, "Failed: download request error"

    local_path.write_bytes(response.content)
    return str(local_path), extension, "Downloaded"


def ensure_inventory_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cbu_bankstats_sources (
            source_id TEXT PRIMARY KEY,
            period_year INTEGER,
            period_month INTEGER,
            report_title TEXT,
            report_page_url TEXT,
            excel_file_url TEXT,
            local_file_path TEXT,
            file_type TEXT,
            downloaded_at TEXT,
            status TEXT,
            notes TEXT
        )
        """
    )


def write_inventory(conn: sqlite3.Connection, records: Iterable[SourceRecord]) -> pd.DataFrame:
    rows = [record.__dict__ for record in records]
    inventory_df = pd.DataFrame(rows)
    if inventory_df.empty:
        inventory_df = pd.DataFrame(
            columns=[
                "source_id",
                "period_year",
                "period_month",
                "report_title",
                "report_page_url",
                "excel_file_url",
                "local_file_path",
                "file_type",
                "downloaded_at",
                "status",
                "notes",
            ]
        )
    inventory_df.to_sql("cbu_bankstats_sources", conn, if_exists="replace", index=False)
    INVENTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    inventory_df.to_csv(INVENTORY_CSV, index=False)
    return inventory_df


def parse_downloaded_excels(inventory_df: pd.DataFrame) -> pd.DataFrame:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict] = []

    for _, row in inventory_df.iterrows():
        file_path = Path(row["local_file_path"]) if row.get("local_file_path") else None
        if not file_path or not file_path.exists() or row.get("status") != "downloaded":
            summary_rows.append(
                {
                    "file_name": file_path.name if file_path else "",
                    "sheet_name": "",
                    "number_of_rows": 0,
                    "number_of_columns": 0,
                    "columns_detected": "",
                    "parse_status": "skipped",
                    "notes": "File missing or not downloaded",
                }
            )
            continue

        try:
            excel = pd.ExcelFile(file_path)
            for sheet_name in excel.sheet_names:
                try:
                    df = excel.parse(sheet_name=sheet_name)
                    out_name = f"{file_path.stem}__{sanitize_filename(sheet_name)}.csv"
                    out_path = PROCESSED_DIR / out_name
                    df.to_csv(out_path, index=False)
                    summary_rows.append(
                        {
                            "file_name": file_path.name,
                            "sheet_name": sheet_name,
                            "number_of_rows": len(df),
                            "number_of_columns": len(df.columns),
                            "columns_detected": " | ".join(str(col) for col in df.columns[:20]),
                            "parse_status": "parsed",
                            "notes": "",
                        }
                    )
                except Exception as exc:  # beginner-friendly broad handling for varied files
                    summary_rows.append(
                        {
                            "file_name": file_path.name,
                            "sheet_name": sheet_name,
                            "number_of_rows": 0,
                            "number_of_columns": 0,
                            "columns_detected": "",
                            "parse_status": "failed",
                            "notes": str(exc),
                        }
                    )
        except Exception as exc:
            summary_rows.append(
                {
                    "file_name": file_path.name,
                    "sheet_name": "",
                    "number_of_rows": 0,
                    "number_of_columns": 0,
                    "columns_detected": "",
                    "parse_status": "failed",
                    "notes": f"Could not open Excel file: {exc}",
                }
            )

    summary_df = pd.DataFrame(summary_rows)
    PARSE_SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(PARSE_SUMMARY_CSV, index=False)
    return summary_df


def collect_cbu_april_2026() -> tuple[pd.DataFrame, pd.DataFrame]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging()
    logging.info("Starting CBU bankstats controlled collection for April 2026")

    records: list[SourceRecord] = []
    with requests.Session() as session:
        monthly_response = safe_get(session, MONTHLY_URL)
        if monthly_response is None:
            logging.error("Could not load monthly page. Collection aborted.")
            return pd.DataFrame(), pd.DataFrame()

        report_links = extract_report_links(monthly_response.text)
        logging.info("Found %d report pages", len(report_links))

        for report_url in report_links:
            report_response = safe_get(session, report_url)
            if report_response is None:
                records.append(
                    SourceRecord(
                        source_id=f"2026_04_{hash(report_url) & 0xffffffff}",
                        period_year=TARGET_YEAR,
                        period_month=TARGET_MONTH,
                        report_title="",
                        report_page_url=report_url,
                        excel_file_url="",
                        local_file_path="",
                        file_type="",
                        downloaded_at=datetime.now(timezone.utc).isoformat(),
                        status="failed",
                        notes="Could not load report page",
                    )
                )
                continue

            report_title = get_report_title(report_response.text)
            excel_links = extract_excel_links(report_response.text)
            report_id_match = re.search(r"(\d+)/?$", report_url)
            report_id = report_id_match.group(1) if report_id_match else "unknown"

            if not excel_links:
                records.append(
                    SourceRecord(
                        source_id=f"2026_04_{report_id}_0",
                        period_year=TARGET_YEAR,
                        period_month=TARGET_MONTH,
                        report_title=report_title,
                        report_page_url=report_url,
                        excel_file_url="",
                        local_file_path="",
                        file_type="",
                        downloaded_at=datetime.now(timezone.utc).isoformat(),
                        status="no_excel_link",
                        notes="No .xlsx/.xls link found on report page",
                    )
                )
                continue

            for idx, excel_url in enumerate(excel_links, start=1):
                local_file_path, file_type, note = download_excel(session, excel_url, report_id, idx)
                status = "downloaded" if note == "Downloaded" else "failed"
                records.append(
                    SourceRecord(
                        source_id=f"2026_04_{report_id}_{idx}",
                        period_year=TARGET_YEAR,
                        period_month=TARGET_MONTH,
                        report_title=report_title,
                        report_page_url=report_url,
                        excel_file_url=excel_url,
                        local_file_path=local_file_path,
                        file_type=file_type,
                        downloaded_at=datetime.now(timezone.utc).isoformat(),
                        status=status,
                        notes=note,
                    )
                )

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        ensure_inventory_table(conn)
        inventory_df = write_inventory(conn, records)

    parse_summary_df = parse_downloaded_excels(inventory_df)
    logging.info("Collection finished. Inventory rows=%d, parse rows=%d", len(inventory_df), len(parse_summary_df))
    return inventory_df, parse_summary_df
