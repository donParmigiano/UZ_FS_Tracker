"""SQLite helpers for loading dashboard data."""

import sqlite3
from typing import Optional

import pandas as pd

from src.common.constants import DB_PATH


def db_exists() -> bool:
    return DB_PATH.exists()


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or str(DB_PATH)
    return sqlite3.connect(path)


def read_table(table_name: str, db_path: Optional[str] = None) -> pd.DataFrame:
    conn = get_connection(db_path)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()
