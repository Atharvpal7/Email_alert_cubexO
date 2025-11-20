"""DAG 1 - clean raw data and hand off to DAG 2."""
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

from src.cleaner import clean_raw_records
from src.db import init_db

default_args = {
    "owner": "data-team",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag1_clean_data",
    description="Create/seed DB and clean raw rows",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "cleaning"],
) as dag:

    init_database = PythonOperator(
        task_id="init_sqlite",
        python_callable=init_db,
        op_kwargs={"reset": False},
    )

    clean_data = PythonOperator(
        task_id="clean_raw_records",
        python_callable=clean_raw_records,
    )

    trigger_validator = TriggerDagRunOperator(
        task_id="trigger_validation_dag",
        trigger_dag_id="dag2_validate_data",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    init_database >> clean_data >> trigger_validator
