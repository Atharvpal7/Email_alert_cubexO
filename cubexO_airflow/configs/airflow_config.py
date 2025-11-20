"""Centralized configuration for the sample Airflow project."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(slots=True)
class PipelineSettings:
    db_path: Path = Path(os.getenv("PIPELINE_DB_PATH", PROJECT_ROOT / "pipeline.db"))
    alert_email: str = os.getenv("PIPELINE_ALERT_EMAIL", "help@theeditor.gmail.com")
    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_user: str = os.getenv("SMTP_USER", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    smtp_from: str = os.getenv("SMTP_FROM", "no-reply@example.com")


settings = PipelineSettings()
