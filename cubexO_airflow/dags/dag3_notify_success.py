"""DAG 3 - email each validated contact."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.notifier import notify_success_for_validated

default_args = {
    "owner": "data-team",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag3_notify_success",
    description="Send success emails for validated rows",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "notification"],
) as dag:

    notify_success = PythonOperator(
        task_id="notify_successful_records",
        python_callable=notify_success_for_validated,
    )
