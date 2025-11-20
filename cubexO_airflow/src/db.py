"""SQLite helpers plus bootstrap logic for the demo pipeline."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from configs.airflow_config import settings

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS invalid_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validated_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    validated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS success_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    validated_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    message TEXT NOT NULL
);
"""

SEED_ROWS = [
    (" Alice  ", "  ALICE@example.com "),
    ("Bob", "bob@example.com"),
    ("  Carol ", "123carol@example.com"),
    ("Dave", "DAVE@Example.com"),
    ("Eve", "   eve_at_example.com"),
    ("Frank", " frank@example.com "),
    ("Grace", "  "),
]


def _ensure_db_dir() -> None:
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    _ensure_db_dir()
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def init_db(reset: bool = False) -> None:
    """Create tables and optionally rebuild sample data."""
    with connect() as conn:
        conn.executescript(SCHEMA_SQL)
        if reset:
            for table in (
                "raw_records",
                "cleaned_records",
                "invalid_records",
                "validated_records",
                "success_logs",
            ):
                conn.execute(f"DELETE FROM {table}")
        _seed_if_empty(conn)
        conn.commit()


def _seed_if_empty(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) as c FROM raw_records").fetchone()
    if row and row[0]:
        return

    conn.executemany(
        "INSERT INTO raw_records (name, email, created_at) VALUES (?, ?, datetime('now'))",
        SEED_ROWS,
    )


__all__ = ["get_connection", "connect", "init_db"]
