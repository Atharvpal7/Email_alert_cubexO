from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def validator_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DB_PATH", str(tmp_path / "test.db"))
    import configs.airflow_config as airflow_config
    import src.db as db
    importlib.reload(airflow_config)
    importlib.reload(db)
    db.init_db(reset=True)
    import src.cleaner as cleaner
    importlib.reload(cleaner)
    cleaner.clean_raw_records()
    import src.validator as validator
    importlib.reload(validator)
    return db, validator


def test_validator_separates_invalid_records(validator_modules):
    db, validator = validator_modules
    msg = validator.validate_cleaned_records()
    assert "Validated" in msg
    with db.connect() as conn:
        invalid_rows = conn.execute("SELECT reason FROM invalid_records").fetchall()
        validated_rows = conn.execute("SELECT email FROM validated_records").fetchall()
    assert len(invalid_rows) > 0
    assert len(validated_rows) > 0
