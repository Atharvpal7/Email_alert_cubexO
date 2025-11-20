"""Data cleaning helpers used by DAG 1."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from .db import connect


def clean_raw_records() -> str:
    """Clean raw rows and store them in cleaned_records."""
    with connect() as conn:
        df = pd.read_sql_query(
            "SELECT id AS raw_id, name, email, created_at FROM raw_records", conn
        )

        if df.empty:
            conn.execute("DELETE FROM cleaned_records")
            conn.commit()
            return "No raw rows to clean."

        df["name"] = df["name"].fillna("").str.strip()
        df["email"] = df["email"].fillna("").str.strip().str.lower()
        df["created_at"] = df["created_at"].fillna(datetime.utcnow().isoformat())

        df = df[df["email"] != ""].copy()

        conn.execute("DELETE FROM cleaned_records")
        df.to_sql("cleaned_records", conn, if_exists="append", index=False)
        conn.commit()

        return f"Cleaned {len(df)} row(s)."


__all__ = ["clean_raw_records"]
