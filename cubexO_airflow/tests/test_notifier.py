from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def prepared_notifier(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DB_PATH", str(tmp_path / "test.db"))
    import configs.airflow_config as airflow_config
    import src.db as db
    importlib.reload(airflow_config)
    importlib.reload(db)
    db.init_db(reset=True)

    import src.cleaner as cleaner
    import src.validator as validator
    importlib.reload(cleaner)
    importlib.reload(validator)

    cleaner.clean_raw_records()
    validator.validate_cleaned_records()

    import src.notifier as notifier
    importlib.reload(notifier)
    return db, notifier


def test_notifier_logs_success(monkeypatch, prepared_notifier):
    db, notifier = prepared_notifier
    sent_to = []
    monkeypatch.setattr(notifier, "send_email", lambda to, subject, body: sent_to.append(to) or True)
    result = notifier.notify_success_for_validated()
    assert "Sent" in result
    assert sent_to
    with db.connect() as conn:
        logged = conn.execute("SELECT COUNT(*) FROM success_logs").fetchone()[0]
    assert logged == len(sent_to)
