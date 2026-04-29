#!/usr/bin/env python3
"""
Collect CBU bank statistics reports for a single month and parse Excel files to CSV.

Examples:
    python collect_cbu_bankstats_month.py --year 2023 --month 04
    python collect_cbu_bankstats_month.py --year 2023 --month 04 --overwrite
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://cbu.uz"
BANKSTATS_SECTION_FILTER = "3497"
TEST_MONTH_URL = "https://cbu.uz/en/statistics/bankstats/978060/"
USER_AGENT = "Mozilla/5.0 (compatible; cbu-bankstats-month-collector/1.0)"
TIMEOUT_SECONDS = 30


@dataclass
class DownloadItem:
    report_url: str
    file_url: str
    file_name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect one month of CBU bankstats excel files.")
    parser.add_argument("--year", type=int, required=True, help="Year, e.g. 2023")
    parser.add_argument("--month", type=int, required=True, help="Month number, e.g. 4 or 04")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing files")
    return parser.parse_args()


def validate_inputs(year: int, month: int) -> None:
    if month < 1 or month > 12:
        raise ValueError("Month must be from 1 to 12.")
    if (year, month) < (2023, 4):
        raise ValueError("Supported start month is April 2023 (2023-04).")


def month_slug(year: int, month: int) -> str:
    return f"{year}_{month:02d}"


def build_month_listing_url(year: int, month: int) -> str:
    query = urlencode({"arFilter_ff[SECTION_ID]": BANKSTATS_SECTION_FILTER, "year": year, "month": f"{month:02d}"})
    return f"{BASE_URL}/en/statistics/bankstats/?{query}"


def new_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_html(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def looks_like_month(url: str, year: int, month: int) -> bool:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    year_q = query.get("year", [None])[0]
    month_q = query.get("month", [None])[0]
    if year_q and month_q:
        return str(year_q) == str(year) and str(month_q).zfill(2) == f"{month:02d}"

    lowered = url.lower()
    return str(year) in lowered and f"{month:02d}" in lowered


def discover_report_pages(session: requests.Session, year: int, month: int) -> list[str]:
    urls_to_visit = [TEST_MONTH_URL, build_month_listing_url(year, month)]
    report_urls: set[str] = set()

    for listing_url in urls_to_visit:
        print(f"Scanning listing page: {listing_url}")
        try:
            soup = fetch_html(session, listing_url)
        except Exception as exc:
            print(f"  Warning: failed to load listing page: {exc}")
            continue

        for a_tag in soup.select("a[href]"):
            href = a_tag.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(BASE_URL, href)
            if "/statistics/bankstats/" not in absolute:
                continue
            if absolute.endswith((".xlsx", ".xls")):
                continue
            if not looks_like_month(absolute, year, month) and listing_url != TEST_MONTH_URL:
                continue
            report_urls.add(absolute.split("#")[0])

    return sorted(report_urls)


def _is_excel_link(value: str) -> bool:
    cleaned = value.strip().strip("\"'<>")
    if not cleaned:
        return False

    parsed = urlparse(cleaned)
    path = unquote(parsed.path).lower()
    return path.endswith(".xlsx") or path.endswith(".xls")


def discover_excel_links(session: requests.Session, report_url: str) -> list[str]:
    soup = fetch_html(session, report_url)
    links: set[str] = set()

    for a_tag in soup.select("a[href]"):
        href = a_tag.get("href", "").strip()
        if not href or not _is_excel_link(href):
            continue
        links.add(urljoin(report_url, href))

    html = soup.decode()
    excel_pattern = re.compile(r"(?:https?://|/|\.\.?/)[^\s\"'<>]+?\.xls(?:x)?(?:\?[^\s\"'<>]*)?", re.IGNORECASE)
    for candidate in excel_pattern.findall(html):
        if not _is_excel_link(candidate):
            continue
        links.add(urljoin(report_url, candidate.strip()))

    return sorted(links)


def safe_filename(url: str, used: set[str]) -> str:
    name = Path(urlparse(url).path).name or "file.xls"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if name in used:
        stem = Path(name).stem
        suffix = Path(name).suffix
        counter = 2
        while f"{stem}_{counter}{suffix}" in used:
            counter += 1
        name = f"{stem}_{counter}{suffix}"
    used.add(name)
    return name


def download_files(session: requests.Session, items: Iterable[DownloadItem], raw_dir: Path, overwrite: bool) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []

    for item in items:
        output_path = raw_dir / item.file_name
        if output_path.exists() and not overwrite:
            print(f"Skip existing raw file: {output_path.name}")
            downloaded.append(output_path)
            continue

        print(f"Downloading: {item.file_url}")
        response = session.get(item.file_url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        downloaded.append(output_path)

    return downloaded


def read_excel_sheets(excel_path: Path) -> dict[str, pd.DataFrame]:
    suffix = excel_path.suffix.lower()
    engine = None
    if suffix == ".xls":
        engine = "xlrd"
    elif suffix == ".xlsx":
        engine = "openpyxl"

    excel_file = pd.ExcelFile(excel_path, engine=engine)
    result: dict[str, pd.DataFrame] = {}
    for sheet in excel_file.sheet_names:
        result[sheet] = excel_file.parse(sheet_name=sheet)
    return result


def parse_to_csv(raw_files: list[Path], processed_dir: Path, overwrite: bool) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    processed_dir.mkdir(parents=True, exist_ok=True)
    inventory_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []
    qa_rows: list[dict[str, str]] = []

    for raw_path in raw_files:
        try:
            sheets = read_excel_sheets(raw_path)
            sheet_count = 0
            total_rows = 0

            for sheet_name, df in sheets.items():
                sheet_count += 1
                total_rows += len(df)
                safe_sheet = re.sub(r"[^A-Za-z0-9._-]+", "_", sheet_name)[:80]
                csv_name = f"{raw_path.stem}__{safe_sheet}.csv"
                csv_path = processed_dir / csv_name

                if csv_path.exists() and not overwrite:
                    status = "skipped_existing"
                else:
                    df.to_csv(csv_path, index=False)
                    status = "written"

                summary_rows.append(
                    {
                        "raw_file": raw_path.name,
                        "sheet_name": sheet_name,
                        "csv_file": csv_name,
                        "rows": str(len(df)),
                        "columns": str(len(df.columns)),
                        "status": status,
                    }
                )

            qa_rows.append(
                {
                    "raw_file": raw_path.name,
                    "parse_ok": "yes",
                    "sheet_count": str(sheet_count),
                    "total_rows": str(total_rows),
                    "error": "",
                }
            )
            inventory_rows.append({"raw_file": raw_path.name, "exists": "yes"})

        except Exception as exc:
            qa_rows.append(
                {
                    "raw_file": raw_path.name,
                    "parse_ok": "no",
                    "sheet_count": "0",
                    "total_rows": "0",
                    "error": str(exc),
                }
            )
            inventory_rows.append({"raw_file": raw_path.name, "exists": "yes"})

    return inventory_rows, summary_rows, qa_rows


def write_csv_rows(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    args = parse_args()
    validate_inputs(args.year, args.month)
    ym = month_slug(args.year, args.month)

    raw_dir = Path("data/raw/cbu_bankstats") / ym
    processed_dir = Path("data/processed/cbu_bankstats") / ym

    session = new_session()
    report_pages = discover_report_pages(session, args.year, args.month)
    print(f"Report pages found: {len(report_pages)}")

    file_name_used: set[str] = set()
    items: list[DownloadItem] = []
    for report_url in report_pages:
        try:
            excel_links = discover_excel_links(session, report_url)
        except Exception as exc:
            print(f"Warning: failed to scan report page {report_url}: {exc}")
            continue

        print(f"Excel links found on report page {report_url}: {len(excel_links)}")
        if not excel_links:
            print(f"Warning: no Excel links found on report page: {report_url}")

        for file_url in excel_links:
            file_name = safe_filename(file_url, file_name_used)
            items.append(DownloadItem(report_url=report_url, file_url=file_url, file_name=file_name))

    print(f"Excel files found: {len(items)}")
    raw_files = download_files(session, items, raw_dir, args.overwrite)

    inventory_rows, summary_rows, qa_rows = parse_to_csv(raw_files, processed_dir, args.overwrite)

    write_csv_rows(
        Path(f"data/processed/cbu_bankstats_inventory_{ym}.csv"),
        inventory_rows,
        headers=["raw_file", "exists"],
    )
    write_csv_rows(
        Path(f"data/processed/cbu_bankstats_parse_summary_{ym}.csv"),
        summary_rows,
        headers=["raw_file", "sheet_name", "csv_file", "rows", "columns", "status"],
    )
    write_csv_rows(
        Path(f"data/processed/cbu_bankstats_parse_qa_{ym}.csv"),
        qa_rows,
        headers=["raw_file", "parse_ok", "sheet_count", "total_rows", "error"],
    )

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
