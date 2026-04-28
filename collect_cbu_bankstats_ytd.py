"""Collector for 2026 YTD (Jan-Apr) CBU banking statistics reports.

Features:
- Collect months: 2026_01, 2026_02, 2026_03, 2026_04.
- Download raw Excel files under data/raw/cbu_bankstats/YYYY_MM/.
- Parse all Excel sheets into CSV under data/processed/cbu_bankstats/YYYY_MM/.
- Safe reruns:
  * skips existing raw files unless --overwrite
  * skips existing parsed CSV files unless --overwrite
  * --parse-only parses existing raw files without downloading
- Exports YTD inventory, parse summary, parse QA, and cell-level master files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import pandas as pd

BASE_URL = "https://cbu.uz"
MONTHS = ["2026_01", "2026_02", "2026_03", "2026_04"]

RAW_BASE_DIR = Path("data/raw/cbu_bankstats")
PROCESSED_BASE_DIR = Path("data/processed/cbu_bankstats")
INVENTORY_CSV = Path("data/processed/cbu_bankstats_inventory_2026_ytd.csv")
PARSE_SUMMARY_CSV = Path("data/processed/cbu_bankstats_parse_summary_2026_ytd.csv")
PARSE_QA_CSV = Path("data/processed/cbu_bankstats_parse_qa_2026_ytd.csv")
CELLS_MASTER_CSV = Path("data/master/cbu_bankstats_cells_master_2026_ytd.csv")

REPORT_LINK_PATTERN = re.compile(r'href=["\']([^"\']+/statistics/bankstats/[^"\']+)["\']', re.IGNORECASE)
FILE_LINK_PATTERN = re.compile(r'href=["\']([^"\']+\.(?:xlsx|xls))(?:\?[^"\']*)?["\']', re.IGNORECASE)


@dataclass
class SourceRecord:
    month: str
    listing_url: str
    report_url: str
    file_url: str
    local_path: str
    download_status: str
    parse_status: str
    error_message: str
    rows_total: int = 0
    sheets_total: int = 0
    csv_files_total: int = 0


@dataclass
class QARow:
    month: str
    file_name: str
    sheet_name: str
    csv_path: str
    rows: int
    cols: int
    null_cells: int
    duplicate_rows: int


def build_listing_url(month: str) -> str:
    year, mm = month.split("_")
    return (
        "https://cbu.uz/en/statistics/bankstats/"
        "?arFilter_DATE_ACTIVE_FROM_1=&arFilter_DATE_ACTIVE_FROM_2="
        f"&arFilter_ff%5BSECTION_ID%5D=3497&year={year}&month={mm}&set_filter=&set_filter=Y"
    )


def fetch_text(url: str, timeout: int = 45) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Collector-YTD/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def fetch_bytes(url: str, timeout: int = 90) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CBU-Collector-YTD/1.0)"})
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


def safe_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "download.xlsx"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if len(cleaned) > 140:
        cleaned = cleaned[:140]
    ext = Path(cleaned).suffix or ".xlsx"
    stem = Path(cleaned).stem
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
    return f"{stem}_{digest}{ext}"


def safe_sheet_slug(sheet_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (sheet_name or "sheet").strip())[:80] or "sheet"


def ensure_dirs(month: str) -> tuple[Path, Path]:
    raw_dir = RAW_BASE_DIR / month
    processed_dir = PROCESSED_BASE_DIR / month
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    INVENTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    PARSE_SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    PARSE_QA_CSV.parent.mkdir(parents=True, exist_ok=True)
    CELLS_MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
    return raw_dir, processed_dir


def parse_excel_to_csv(
    local_file: Path,
    processed_dir: Path,
    month: str,
    overwrite: bool,
) -> tuple[int, int, int, list[QARow], list[dict[str, object]], bool]:
    excel = pd.ExcelFile(local_file)
    total_rows = 0
    total_sheets = 0
    total_csv_files = 0
    qa_rows: list[QARow] = []
    cell_rows: list[dict[str, object]] = []
    any_skipped = False

    for sheet in excel.sheet_names:
        df = excel.parse(sheet_name=sheet)
        sheet_slug = safe_sheet_slug(sheet)
        out_path = processed_dir / f"{local_file.stem}__{sheet_slug}.csv"

        if out_path.exists() and not overwrite:
            any_skipped = True
        else:
            df.to_csv(out_path, index=False)

        rows = len(df)
        cols = len(df.columns)
        null_cells = int(df.isna().sum().sum())
        duplicate_rows = int(df.duplicated().sum())

        qa_rows.append(
            QARow(
                month=month,
                file_name=local_file.name,
                sheet_name=sheet,
                csv_path=str(out_path),
                rows=rows,
                cols=cols,
                null_cells=null_cells,
                duplicate_rows=duplicate_rows,
            )
        )

        for row_idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
            for col_idx, value in enumerate(row, start=1):
                if pd.notna(value):
                    cell_rows.append(
                        {
                            "month": month,
                            "file_name": local_file.name,
                            "sheet_name": sheet,
                            "row_number": row_idx,
                            "column_number": col_idx,
                            "column_name": str(df.columns[col_idx - 1]),
                            "cell_value": str(value),
                        }
                    )

        total_rows += rows
        total_sheets += 1
        total_csv_files += 1

    return total_rows, total_sheets, total_csv_files, qa_rows, cell_rows, any_skipped


def export_inventory(records: list[SourceRecord]) -> None:
    with INVENTORY_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "month",
                "listing_url",
                "report_url",
                "file_url",
                "local_path",
                "download_status",
                "parse_status",
                "rows_total",
                "sheets_total",
                "csv_files_total",
                "error_message",
            ],
        )
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))


def export_parse_summary(records: list[SourceRecord]) -> None:
    rows: list[dict[str, int | str]] = []
    for month in MONTHS:
        monthly_records = [r for r in records if r.month == month]
        rows.append(
            {
                "month": month,
                "files_found": len(monthly_records),
                "files_downloaded": sum(r.download_status in {"downloaded", "skipped_existing", "parse_only"} for r in monthly_records),
                "files_parsed": sum(r.parse_status in {"parsed", "skipped_existing"} for r in monthly_records),
                "sheets_parsed": sum(r.sheets_total for r in monthly_records),
                "rows_parsed": sum(r.rows_total for r in monthly_records),
                "csv_files_total": sum(r.csv_files_total for r in monthly_records),
            }
        )

    total = {
        "month": "TOTAL",
        "files_found": sum(r["files_found"] for r in rows),
        "files_downloaded": sum(r["files_downloaded"] for r in rows),
        "files_parsed": sum(r["files_parsed"] for r in rows),
        "sheets_parsed": sum(r["sheets_parsed"] for r in rows),
        "rows_parsed": sum(r["rows_parsed"] for r in rows),
        "csv_files_total": sum(r["csv_files_total"] for r in rows),
    }
    rows.append(total)
    pd.DataFrame(rows).to_csv(PARSE_SUMMARY_CSV, index=False)


def export_parse_qa(qa_rows: list[QARow]) -> None:
    columns = ["month", "file_name", "sheet_name", "csv_path", "rows", "cols", "null_cells", "duplicate_rows"]
    pd.DataFrame([asdict(row) for row in qa_rows], columns=columns).to_csv(PARSE_QA_CSV, index=False)


def export_cells_master(cell_rows: list[dict[str, object]]) -> None:
    columns = ["month", "file_name", "sheet_name", "row_number", "column_number", "column_name", "cell_value"]
    pd.DataFrame(cell_rows, columns=columns).to_csv(CELLS_MASTER_CSV, index=False)


def collect_month(month: str, parse_only: bool, overwrite: bool) -> tuple[list[SourceRecord], list[QARow], list[dict[str, object]]]:
    listing_url = build_listing_url(month)
    raw_dir, processed_dir = ensure_dirs(month)

    records: list[SourceRecord] = []
    qa_rows: list[QARow] = []
    cell_rows: list[dict[str, object]] = []

    if parse_only:
        raw_files = sorted([p for p in raw_dir.glob("*.xls*") if p.is_file()])
        for local_file in raw_files:
            rec = SourceRecord(
                month=month,
                listing_url=listing_url,
                report_url="",
                file_url="",
                local_path=str(local_file),
                download_status="parse_only",
                parse_status="pending",
                error_message="",
            )
            try:
                rows_total, sheets_total, csv_total, qa, cells, skipped = parse_excel_to_csv(local_file, processed_dir, month, overwrite)
                rec.rows_total = rows_total
                rec.sheets_total = sheets_total
                rec.csv_files_total = csv_total
                rec.parse_status = "skipped_existing" if skipped and not overwrite else "parsed"
                qa_rows.extend(qa)
                cell_rows.extend(cells)
            except Exception as exc:
                rec.parse_status = "failed"
                rec.error_message = f"parse_error: {exc}"
            records.append(rec)
        return records, qa_rows, cell_rows

    try:
        listing_html = fetch_text(listing_url)
        report_links = extract_report_links(listing_html)
    except Exception as exc:
        records.append(
            SourceRecord(
                month=month,
                listing_url=listing_url,
                report_url="",
                file_url="",
                local_path="",
                download_status="failed",
                parse_status="failed",
                error_message=f"listing_open_error: {exc}",
            )
        )
        return records, qa_rows, cell_rows

    for report_url in report_links:
        try:
            report_html = fetch_text(report_url)
            file_links = extract_excel_links(report_html, report_url)
        except Exception as exc:
            records.append(
                SourceRecord(
                    month=month,
                    listing_url=listing_url,
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
            local_file = raw_dir / safe_filename_from_url(file_url)
            rec = SourceRecord(
                month=month,
                listing_url=listing_url,
                report_url=report_url,
                file_url=file_url,
                local_path=str(local_file),
                download_status="pending",
                parse_status="pending",
                error_message="",
            )

            if local_file.exists() and not overwrite:
                rec.download_status = "skipped_existing"
            else:
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
                rows_total, sheets_total, csv_total, qa, cells, skipped = parse_excel_to_csv(local_file, processed_dir, month, overwrite)
                rec.rows_total = rows_total
                rec.sheets_total = sheets_total
                rec.csv_files_total = csv_total
                rec.parse_status = "skipped_existing" if skipped and not overwrite else "parsed"
                qa_rows.extend(qa)
                cell_rows.extend(cells)
            except Exception as exc:
                rec.parse_status = "failed"
                rec.error_message = f"parse_error: {exc}"

            records.append(rec)

    return records, qa_rows, cell_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect 2026 YTD CBU banking statistics")
    parser.add_argument("--parse-only", action="store_true", help="Only parse existing raw Excel files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing raw and parsed outputs")
    return parser.parse_args()


def run(parse_only: bool, overwrite: bool) -> None:
    all_records: list[SourceRecord] = []
    all_qa_rows: list[QARow] = []
    all_cell_rows: list[dict[str, object]] = []

    for month in MONTHS:
        records, qa_rows, cell_rows = collect_month(month, parse_only=parse_only, overwrite=overwrite)
        all_records.extend(records)
        all_qa_rows.extend(qa_rows)
        all_cell_rows.extend(cell_rows)

    export_inventory(all_records)
    export_parse_summary(all_records)
    export_parse_qa(all_qa_rows)
    export_cells_master(all_cell_rows)

    print(
        "Completed 2026 YTD collection:",
        f"records={len(all_records)} qa_rows={len(all_qa_rows)} master_cells={len(all_cell_rows)}",
    )


if __name__ == "__main__":
    args = parse_args()
    run(parse_only=args.parse_only, overwrite=args.overwrite)
