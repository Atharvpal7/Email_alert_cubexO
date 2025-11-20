"""DAG 2 - validate cleaned data and notify invalid rows."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.validator import validate_cleaned_records

default_args = {
    "owner": "data-team",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag2_validate_data",
    description="Validate cleaned rows, move valid rows, alert on invalid",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "validation"],
) as dag:

    validate_data = PythonOperator(
        task_id="validate_cleaned_records",
        python_callable=validate_cleaned_records,
    )

    trigger_notifier = TriggerDagRunOperator(
        task_id="trigger_notify_success_dag",
        trigger_dag_id="dag3_notify_success",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    validate_data >> trigger_notifier
