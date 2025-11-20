# Airflow Data Pipeline Demo

Three Apache Airflow DAGs demonstrate a realistic cleaning/validation workflow. A lightweight SQLite database is bundled so the project works out-of-the-box.

## What is included

- DAG 1 (`dag1_clean_data`): initializes/loads the SQLite database and cleans `raw_records` with pandas.
- DAG 2 (`dag2_validate_data`): validates email addresses, moves good rows to `validated_records`, and emails the editor when data looks bad.
- DAG 3 (`dag3_notify_success`): emails each validated contact and logs the attempt in `success_logs`.
- Helper modules in `src/` for database access, cleaning, validation, and notification logic.
- Config file in `configs/airflow_config.py` to centralize filesystem paths and SMTP settings.
- Pytest unit tests that exercise the helpers without running Airflow itself.

## Requirements

- Python 3.10+
- Apache Airflow 2.7+
- SQLite (ships with Python)
- Pandas for the cleaning step

All dependencies are listed in `requirements.txt`.

## Getting started

### Option A – Docker Compose (recommended)

1. Install Docker Desktop.
2. From the project root run:

   ```bash
   docker-compose up --build
   ```

3. Open http://localhost:8080 for the Airflow UI (user: `admin`, password: `admin`).
4. Trigger `dag1_clean_data`; DAG 2 and 3 will chain automatically.

Environment variables such as `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, and `SMTP_FROM` can be provided via a `.env` file or the shell before running Compose.

### Option B – Local Python environment

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Airflow needs a home directory. The quickest setup is:

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
```

Then copy/link the DAGs:

```bash
mkdir -p "$AIRFLOW_HOME/dags"
cp dags/*.py "$AIRFLOW_HOME/dags/"
```

You can now start the scheduler/webserver:

```bash
airflow webserver -p 8080 &
airflow scheduler
```

Inside the Airflow UI, trigger `dag1_clean_data`. It will seed/clean the database and automatically trigger DAG 2, which in turn fires DAG 3.

## SMTP configuration

Same variables apply for both Docker and local runs:

- `SMTP_HOST`
- `SMTP_PORT` (default 587)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_FROM`

If they are omitted, the notifier switches to a dry-run mode that only logs outgoing emails, so you can still test the DAG flow.

## Running the tests

```bash
pytest
```

The tests spin up their own temporary SQLite database files, so they will not clobber the main demo database.
