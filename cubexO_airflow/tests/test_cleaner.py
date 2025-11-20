from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def cleaner_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DB_PATH", str(tmp_path / "test.db"))
    import configs.airflow_config as airflow_config
    import src.db as db
    importlib.reload(airflow_config)
    importlib.reload(db)
    db.init_db(reset=True)
    import src.cleaner as cleaner
    importlib.reload(cleaner)
    return db, cleaner


def test_cleaner_drops_empty_emails(cleaner_modules):
    db, cleaner = cleaner_modules
    result = cleaner.clean_raw_records()
    assert "Cleaned" in result
    with db.connect() as conn:
        cleaned = conn.execute("SELECT email FROM cleaned_records").fetchall()
    assert all(row[0].strip() for row in cleaned)
