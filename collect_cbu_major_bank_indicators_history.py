"""Collect historical CBU major bank indicators Excel files.

Run:
    python collect_cbu_major_bank_indicators_history.py
    python collect_cbu_major_bank_indicators_history.py --year-start 2019 --year-end 2026 --workers 5
    python collect_cbu_major_bank_indicators_history.py --overwrite
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://cbu.uz"
BANKSTATS_PATH = "/en/statistics/bankstats/"
SECTION_ID = "3497"
DEFAULT_FILTER_YEAR = 2026
DEFAULT_FILTER_MONTH = 1
RAW_ROOT = Path("data/raw/cbu_bankstats")
REPORT_PATH = Path("data/processed/cbu_major_bank_indicators_collection_report.csv")
SUMMARY_PATH = Path("data/processed/cbu_major_bank_indicators_collection_summary.csv")


@dataclass
class CollectionRow:
    period_year: int
    period_month: int
    period: str
    listing_url: str
    report_page_url: str
    report_title_detected: str
    excel_file_url: str
    local_file_path: str
    status: str
    error_message: str
    collected_at: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect historical CBU major bank indicator files.")
    parser.add_argument("--year-start", type=int, default=2019)
    parser.add_argument("--year-end", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "UZ-FS-Tracker/1.0 (+historical-collector; respectful-crawl)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def fetch_text(session: requests.Session, url: str, timeout: float = 12.0, retries: int = 3) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch URL: {url}; error={last_error}")


def fetch_bytes(session: requests.Session, url: str, timeout: float = 20.0, retries: int = 3) -> bytes:
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.content
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.7 * (attempt + 1))
    raise RuntimeError(f"Failed to download URL: {url}; error={last_error}")


def build_listing_url(year: int, month: int) -> str:
    month_str = f"{month:02d}"
    next_year = year + 1 if month == 12 else year
    next_month = 1 if month == 12 else month + 1
    query_items = [
        ("year", f"{DEFAULT_FILTER_YEAR:04d}"),
        ("month", f"{DEFAULT_FILTER_MONTH:02d}"),
        ("arFilter_DATE_ACTIVE_FROM_1", f"01.{month_str}.{year:04d}"),
        ("arFilter_DATE_ACTIVE_FROM_2", f"01.{next_month:02d}.{next_year:04d}"),
        ("arFilter_ff[SECTION_ID]", SECTION_ID),
        ("year", f"{year:04d}"),
        ("month", month_str),
        ("set_filter", ""),
        ("set_filter", "Y"),
    ]
    return f"{BASE_URL}{BANKSTATS_PATH}?{urlencode(query_items)}"


def is_valid_report_page(url: str) -> bool:
    path = urlparse(url).path
    return bool(re.fullmatch(r"/(?:en/)?statistics/bankstats/\d+/", path))


def extract_candidate_report_links(listing_html: str, listing_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    results: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        full_url = urljoin(listing_url, href)
        if is_valid_report_page(full_url):
            results.add(full_url)
    for match in re.findall(r"""(?:"|')(\/(?:en\/)?statistics\/bankstats\/\d+\/)(?:\?|#|(?:"|'))""", listing_html):
        full_url = urljoin(listing_url, match)
        if is_valid_report_page(full_url):
            results.add(full_url)
    found = sorted(results)
    print(f"[SCAN] listing={listing_url}")
    print(f"[SCAN] report_pages_found={len(found)}")
    print(f"[SCAN] report_page_urls={found}")
    return found


def extract_page_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    return ""


def extract_excel_links(page_html: str, page_url: str) -> tuple[str, list[str]]:
    soup = BeautifulSoup(page_html, "html.parser")
    page_title = extract_page_title(soup)
    excel_links: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        full = urljoin(page_url, href)
        path = urlparse(full).path.lower()
        if path.endswith(".xlsx") or path.endswith(".xls"):
            excel_links.add(full)
    if not excel_links:
        for match in re.findall(r"""(?:"|')(https?://[^\s"'<>]+?\.(?:xlsx|xls)(?:\?[^\s"'<>]*)?|/(?:[^\s"'<>]+?\.xls(?:x)?(?:\?[^\s"'<>]*)?))(?:["'])""", page_html, flags=re.IGNORECASE):
            excel_links.add(urljoin(page_url, match))

    return page_title, sorted(excel_links)


def filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    if name:
        return name
    return f"download_{quote_plus(url)}.xlsx"


def collect_period(year: int, month: int, overwrite: bool) -> list[CollectionRow]:
    session = create_session()
    now = datetime.now(timezone.utc).isoformat()
    period = f"{year:04d}-{month:02d}"
    listing_url = build_listing_url(year, month)

    try:
        listing_html = fetch_text(session, listing_url)
    except Exception as exc:  # noqa: BLE001
        return [
            CollectionRow(year, month, period, listing_url, "", "", "", "", "error", str(exc), now)
        ]

    candidates = extract_candidate_report_links(listing_html, listing_url)
    rows: list[CollectionRow] = []

    for page_url in candidates:
        try:
            page_html = fetch_text(session, page_url)
            page_title, excel_links = extract_excel_links(page_html, page_url)
            print(f"[PAGE] url={page_url} excel_links_found={len(excel_links)}")
            if not excel_links:
                rows.append(
                    CollectionRow(
                        year,
                        month,
                        period,
                        listing_url,
                        page_url,
                        page_title,
                        "",
                        "",
                        "no_excel_found",
                        "No .xls/.xlsx link found on report page.",
                        now,
                    )
                )
                continue

            for file_url in excel_links:
                out_dir = RAW_ROOT / f"{year:04d}_{month:02d}"
                out_dir.mkdir(parents=True, exist_ok=True)
                local_path = out_dir / filename_from_url(file_url)

                if local_path.exists() and not overwrite:
                    rows.append(
                        CollectionRow(
                            year,
                            month,
                            period,
                            listing_url,
                            page_url,
                            page_title,
                            file_url,
                            str(local_path),
                            "skipped_existing",
                            "",
                            now,
                        )
                    )
                    continue

                try:
                    blob = fetch_bytes(session, file_url)
                    local_path.write_bytes(blob)
                    rows.append(
                        CollectionRow(
                            year,
                            month,
                            period,
                            listing_url,
                            page_url,
                            page_title,
                            file_url,
                            str(local_path),
                            "downloaded",
                            "",
                            now,
                        )
                    )
                    time.sleep(0.2)
                except Exception as download_exc:  # noqa: BLE001
                    rows.append(
                        CollectionRow(
                            year,
                            month,
                            period,
                            listing_url,
                            page_url,
                            page_title,
                            file_url,
                            str(local_path),
                            "error",
                            str(download_exc),
                            now,
                        )
                    )

        except Exception as page_exc:  # noqa: BLE001
            rows.append(
                CollectionRow(
                    year,
                    month,
                    period,
                    listing_url,
                    page_url,
                    "",
                    "",
                    "",
                    "error",
                    str(page_exc),
                    now,
                )
            )

    if not rows:
        rows.append(CollectionRow(year, month, period, listing_url, "", "", "", "", "no_report_pages", "", now))
    return rows


def write_report(rows: list[CollectionRow]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "period_year",
                "period_month",
                "period",
                "listing_url",
                "report_page_url",
                "report_title_detected",
                "excel_file_url",
                "local_file_path",
                "status",
                "error_message",
                "collected_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.period_year,
                    row.period_month,
                    row.period,
                    row.listing_url,
                    row.report_page_url,
                    row.report_title_detected,
                    row.excel_file_url,
                    row.local_file_path,
                    row.status,
                    row.error_message,
                    row.collected_at,
                ]
            )


def write_summary(rows: list[CollectionRow], started_at: str, finished_at: str) -> None:
    periods = {(r.period_year, r.period_month) for r in rows}
    report_pages_found = {r.report_page_url for r in rows if r.report_page_url}
    report_pages_visited = {r.report_page_url for r in rows if r.report_page_url and r.status != "listing_only"}
    excel_found = sum(1 for r in rows if r.excel_file_url or r.status == "no_excel_found")
    downloaded = sum(1 for r in rows if r.status == "downloaded")
    skipped = sum(1 for r in rows if r.status == "skipped_existing")
    errors = sum(1 for r in rows if r.status == "error")

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SUMMARY_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "total_months_scanned",
                "total_report_pages_found",
                "total_report_pages_visited",
                "total_excel_files_found",
                "total_excel_files_downloaded",
                "total_excel_files_skipped_existing",
                "total_errors",
                "run_started_at",
                "run_finished_at",
            ]
        )
        writer.writerow([len(periods), len(report_pages_found), len(report_pages_visited), excel_found, downloaded, skipped, errors, started_at, finished_at])


def main() -> None:
    args = parse_args()
    if args.year_end < args.year_start:
        raise SystemExit("--year-end must be >= --year-start")

    started_at = datetime.now(timezone.utc).isoformat()
    tasks: list[tuple[int, int]] = [(y, m) for y in range(args.year_start, args.year_end + 1) for m in range(1, 13)]
    all_rows: list[CollectionRow] = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(collect_period, year, month, args.overwrite): (year, month) for year, month in tasks}
        for future in as_completed(futures):
            all_rows.extend(future.result())

    all_rows.sort(key=lambda r: (r.period_year, r.period_month, r.report_page_url, r.excel_file_url))
    write_report(all_rows)
    finished_at = datetime.now(timezone.utc).isoformat()
    write_summary(all_rows, started_at, finished_at)

    print(f"Collection rows: {len(all_rows)} -> {REPORT_PATH}")
    print(f"Summary report -> {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
