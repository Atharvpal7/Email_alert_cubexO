"""Email helpers plus success notification orchestration (DAG 3)."""
from __future__ import annotations

import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Optional

from configs.airflow_config import settings
from .db import connect


def _build_client() -> Optional[smtplib.SMTP]:
    if not settings.smtp_host:
        return None
    try:
        client = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10)
        client.starttls()
        if settings.smtp_user and settings.smtp_password:
            client.login(settings.smtp_user, settings.smtp_password)
        return client
    except Exception as exc:  # pragma: no cover - logging is enough here
        print(f"[SMTP] failed to connect: {exc}")
        return None


def send_email(to_email: str, subject: str, body: str) -> bool:
    client = _build_client()
    if client is None:
        # Local dev mode: just log so we can keep working without SMTP creds.
        print(f"[SMTP-DRY-RUN] To={to_email} Subject={subject}\n{body}")
        return False

    try:
        msg = EmailMessage()
        msg["From"] = settings.smtp_from
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.set_content(body)
        client.send_message(msg)
        return True
    finally:
        client.quit()


def send_invalid_row_alert(raw_id: int, email: str, reason: str) -> None:
    subject = f"Invalid record detected (id={raw_id})"
    body = (
        "A record failed validation.\n\n"
        f"Raw ID: {raw_id}\n"
        f"Email: {email}\n"
        f"Reason: {reason}\n"
        f"Detected at: {datetime.utcnow().isoformat()}Z\n"
    )
    send_email(settings.alert_email, subject, body)


def notify_success_for_validated() -> str:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT v.id, v.email, v.name, v.validated_at
            FROM validated_records v
            LEFT JOIN success_logs s ON s.validated_id = v.id
            WHERE s.id IS NULL
            ORDER BY v.id
            """
        ).fetchall()

        if not rows:
            return "No pending success emails."

        sent_count = 0
        now = datetime.utcnow().isoformat() + "Z"
        for row in rows:
            subject = "Data validation complete"
            body = (
                f"Hi {row['name']},\n\n"
                f"Your submission cleared our validation step on {row['validated_at']}.\n"
                "Thanks for staying with us."
            )
            success = send_email(row["email"], subject, body)
            if success:
                sent_count += 1
                conn.execute(
                    "INSERT INTO success_logs (validated_id, email, sent_at, message) VALUES (?, ?, ?, ?)",
                    (
                        row["id"],
                        row["email"],
                        now,
                        f"success mail sent at {now}",
                    ),
                )
        conn.commit()
        return f"Sent {sent_count} success emails."


__all__ = ["send_invalid_row_alert", "notify_success_for_validated", "send_email"]
