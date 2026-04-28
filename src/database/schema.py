"""Database schema management for the MVP SQLite storage."""

CREATE_BANK_MONTHLY = """
CREATE TABLE IF NOT EXISTS bank_monthly (
    date TEXT,
    bank TEXT,
    assets REAL,
    loans REAL,
    deposits REAL,
    capital REAL,
    net_profit REAL,
    roa REAL,
    roe REAL,
    npl_ratio REAL,
    cost_to_income_ratio REAL,
    corporate_loans REAL,
    retail_loans REAL,
    sme_loans REAL,
    deposit_rate REAL,
    loan_rate REAL,
    payment_transactions REAL,
    PRIMARY KEY (date, bank)
);
"""

CREATE_REGION_MONTHLY = """
CREATE TABLE IF NOT EXISTS region_monthly (
    date TEXT,
    region TEXT,
    deposits REAL,
    loans REAL,
    active_cards REAL,
    payment_volume REAL,
    PRIMARY KEY (date, region)
);
"""

CREATE_DATA_CATALOGUE = """
CREATE TABLE IF NOT EXISTS data_catalogue (
    dataset TEXT,
    level TEXT,
    description TEXT,
    is_synthetic INTEGER
);
"""
