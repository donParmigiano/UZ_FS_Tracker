"""Utilities for matching raw CBU Excel file names to canonical report keys."""

from __future__ import annotations

import re
from urllib.parse import unquote


# Canonical report keys with their known file-name aliases.
REPORT_ALIASES = {
    "major_bank_indicators": [
        "information on major indicators of commercial banks",
    ],
    "major_sector_indicators": [
        "information on major indicators of banking sector",
    ],
    "capital_categorization": [
        "categorization of commercial banks in terms of total and authorized capital levels",
    ],
    "capital_adequacy": [
        "capital adequacy of the banking sector",
    ],
    "consolidated_balance": [
        "consolidated balance of commercial banks",
    ],
    "loans_by_economic_sector": [
        "information on commercial bank loans by economic sector",
        "information on commercial bank loans by economic sectors",
    ],
    "bank_loans_deposits": [
        "information on loans and deposits of banks",
        "information on loans and deposits of commercial banks",
    ],
    "bank_npl": [
        "information on non performing loans of commercial banks",
        "information on non_performing loans of commercial banks",
    ],
    "regional_loans_deposits": [
        "information on total loans and total deposits of banking system by regions",
        "information on banking system loan portfolio by regions",
    ],
    "loan_types": [
        "information on the loan portfolio in terms of loan types",
    ],
    "liquidity_dynamics": [
        "liquidity dynamics of banking sector",
    ],
    "profitability_indicators": [
        "profitability indicators of banking sector",
    ],
    "deposit_maturity": [
        "the amount of deposits by maturities",
        "the amount of deposits and deposit certificates",
    ],
    "relative_bank_indicators": [
        "information on relative indicators of banks",
    ],
    "relative_system_indicators": [
        "information on relative indicators of banking system",
        "information on relative indicators of banking system 1",
    ],
    "asset_size_grouped_performance": [
        "main performance indicators of commercial banks grouped in terms of asset size",
        "main performance indicators of banks grouped in terms of asset size",
    ],
    "banking_system_stability": [
        "banking system stability indicators of the republic of uzbekistan",
    ],
    "role_of_banking_sector": [
        "role of banking sector in the economy of uzbekistan",
    ],
    "credit_organizations": [
        "number of credit organizations and bank divisions",
    ],
    "weighted_average_interest_rates_individuals": [
        "weighted average interest rates of loans given by commercial banks to individuals",
    ],
    "weighted_average_interest_rates_legal_entities": [
        "weighted average interest rates of loans given by commercial banks to legal entities",
    ],
}


def is_html_fallback(file_name: str) -> bool:
    """Return True when a raw file name looks like an HTML fallback export."""
    text = unquote(file_name or "").lower()
    return "html_fallback" in text or "html fallback" in text


def normalize_report_name(file_name: str) -> str:
    """Normalize a raw file name so aliases can be matched consistently.

    Steps are intentionally straightforward and beginner-friendly:
    - decode URL text
    - lowercase and remove extension
    - normalize separators/spacing
    - trim common suffix noise
    - fix known misspellings
    """
    text = unquote(file_name or "").strip().lower()

    # Remove extension first so later suffix-cleanup is easier.
    text = re.sub(r"\.(xlsx|xlsm|xls)\s*$", "", text)

    # Normalize separators to spaces.
    text = text.replace("_", " ").replace("-", " ")

    # Known correction from historical filenames.
    text = text.replace("sertificates", "certificates")

    # Remove trailing broad website phrase.
    text = re.sub(
        r"\s*the central bank of the republic of uzbekistan\s*$",
        "",
        text,
    )

    # Remove date phrases after "as of ..." which are non-essential for report identity.
    text = re.sub(r"\s+as of\s+.*$", "", text)

    # Remove common suffix markers.
    text = re.sub(r"\s+(html fallback|en|june|july|aug|september)\s*$", "", text)

    # Remove trailing numeric IDs (e.g. "... 1234").
    text = re.sub(r"\s+\d{2,}\s*$", "", text)

    # Remove trailing hash-like token, e.g. "abc123de".
    text = re.sub(r"\s+[a-f0-9]{7,}\s*$", "", text)

    # Collapse spaces and trim.
    text = re.sub(r"\s+", " ", text).strip()
    return text


def match_report_key(file_name: str) -> dict:
    """Match file name to canonical report key with confidence and notes."""
    normalized_name = normalize_report_name(file_name)

    normalized_aliases: list[tuple[str, str]] = []
    for key, aliases in REPORT_ALIASES.items():
        for alias in aliases:
            normalized_aliases.append((key, normalize_report_name(alias)))

    # High confidence: exact alias equality.
    for key, alias in normalized_aliases:
        if normalized_name == alias:
            return {
                "report_key": key,
                "normalized_name": normalized_name,
                "matched_alias": alias,
                "match_confidence": "high",
                "notes": "Exact normalized alias match",
            }

    # Medium confidence: contains-based matching in either direction.
    for key, alias in normalized_aliases:
        if alias and (alias in normalized_name or normalized_name in alias):
            return {
                "report_key": key,
                "normalized_name": normalized_name,
                "matched_alias": alias,
                "match_confidence": "medium",
                "notes": "Contains-based alias match",
            }

    return {
        "report_key": "unknown",
        "normalized_name": normalized_name,
        "matched_alias": "",
        "match_confidence": "none",
        "notes": "No alias match",
    }
