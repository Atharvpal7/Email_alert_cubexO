"""Validation helpers used by DAG 2."""
from __future__ import annotations

from datetime import datetime
from typing import Tuple

from .db import connect
from .notifier import send_invalid_row_alert


def _is_email_valid(email: str) -> Tuple[bool, str | None]:
    if not email:
        return False, "empty email"
    if email[0].isdigit():
        return False, "starts with digit"
    if "@" not in email:
        return False, "missing @"
    if any(ch.isupper() for ch in email):
        return False, "contains uppercase"
    return True, None


def validate_cleaned_records() -> str:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, raw_id, name, email FROM cleaned_records ORDER BY id"
        ).fetchall()
        if not rows:
            conn.execute("DELETE FROM invalid_records")
            conn.commit()
            return "No cleaned rows to validate."

        conn.execute("DELETE FROM invalid_records")
        existing_valid = {
            row[0]
            for row in conn.execute("SELECT raw_id FROM validated_records")
        }

        valid_count = 0
        invalid_count = 0
        now = datetime.utcnow().isoformat() + "Z"

        for row in rows:
            email = row["email"] or ""
            ok, reason = _is_email_valid(email)
            if not ok:
                invalid_count += 1
                conn.execute(
                    """
                    INSERT INTO invalid_records (raw_id, name, email, reason, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (row["raw_id"], row["name"], email, reason or "invalid", now),
                )
                send_invalid_row_alert(row["raw_id"], email, reason or "invalid")
                continue

            if row["raw_id"] in existing_valid:
                continue

            valid_count += 1
            conn.execute(
                """
                INSERT INTO validated_records (raw_id, name, email, validated_at)
                VALUES (?, ?, ?, ?)
                """,
                (row["raw_id"], row["name"], email, now),
            )
            existing_valid.add(row["raw_id"])

        conn.commit()
        return f"Validated={valid_count}, invalid={invalid_count}."


__all__ = ["validate_cleaned_records"]
