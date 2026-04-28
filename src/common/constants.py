"""Project-wide constants for the Uzbekistan dashboard MVP."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "database" / "uz_banking_demo.sqlite"

BANKS = [
    "National Bank of Uzbekistan",
    "Asaka Bank",
    "Ipoteka Bank",
    "Agrobank",
    "Aloqabank",
    "Xalq Banki",
    "Hamkorbank",
    "Kapitalbank",
    "Orient Finans Bank",
    "TBC Bank Uzbekistan",
    "Anor Bank",
    "Davr Bank",
]

REGIONS = [
    "Tashkent City",
    "Tashkent Region",
    "Andijan",
    "Bukhara",
    "Fergana",
    "Jizzakh",
    "Namangan",
    "Navoiy",
    "Kashkadarya",
    "Samarkand",
    "Sirdarya",
    "Surkhandarya",
    "Khorezm",
    "Republic of Karakalpakstan",
]

BANK_INDICATORS = [
    "assets",
    "loans",
    "deposits",
    "capital",
    "net_profit",
    "roa",
    "roe",
    "npl_ratio",
    "cost_to_income_ratio",
    "corporate_loans",
    "retail_loans",
    "sme_loans",
    "deposit_rate",
    "loan_rate",
    "payment_transactions",
]

REGION_INDICATORS = ["deposits", "loans", "active_cards", "payment_volume"]
