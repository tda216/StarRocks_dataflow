"""Airflow orchestration for the local Hotel Booking BI pipeline."""

from __future__ import annotations

import csv
import os
import socket
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
DATA_PATH = AIRFLOW_HOME / "data" / "input" / "hotel_bookings.csv"
SCRIPTS_DIR = AIRFLOW_HOME / "scripts"
SQL_DIR = AIRFLOW_HOME / "include" / "sql"
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", AIRFLOW_HOME / "dbt" / "hotel_booking"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR))

STARROCKS_HOST = os.environ.get("STARROCKS_HOST", "starrocks")
STARROCKS_QUERY_PORT = int(os.environ.get("STARROCKS_QUERY_PORT", "9030"))
STARROCKS_HTTP_PORT = os.environ.get("STARROCKS_HTTP_PORT", "8030")
STARROCKS_BE_HTTP_PORT = os.environ.get("STARROCKS_BE_HTTP_PORT", "8040")
STARROCKS_USER = os.environ.get("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.environ.get("STARROCKS_PASSWORD", "")
STARROCKS_DATABASE = os.environ.get("STARROCKS_DATABASE", "hotel_booking")
STARROCKS_RAW_TABLE = os.environ.get("STARROCKS_RAW_TABLE", "raw_hotel_bookings")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")

SCRIPT_ENV = {
    "MINIO_EXTERNAL_ENDPOINT": MINIO_ENDPOINT,
    "STARROCKS_EXTERNAL_HOST": STARROCKS_HOST,
    "STARROCKS_EXTERNAL_QUERY_PORT": str(STARROCKS_QUERY_PORT),
    # Direct BE port makes Stream Load fallback more reliable inside Docker.
    "STARROCKS_EXTERNAL_HTTP_PORT": STARROCKS_BE_HTTP_PORT,
    "STARROCKS_SQL_DIR": str(SQL_DIR),
}

MART_TABLES = [
    "fact_bookings",
    "mart_daily_booking_revenue",
    "mart_monthly_booking_revenue",
    "mart_hotel_performance",
    "mart_room_performance",
    "mart_market_segment_performance",
    "mart_channel_performance",
    "mart_country_performance",
    "mart_cancellation_analysis",
    "mart_lead_time_analysis",
    "mart_customer_type_performance",
]


def _starrocks_connection(database: str | None = None):
    import pymysql

    return pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_QUERY_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        database=database,
        autocommit=True,
        charset="utf8mb4",
    )


def _split_sql(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False

    for char in sql_text:
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def _execute_sql_file(filename: str) -> None:
    path = SQL_DIR / filename
    if not path.exists():
        raise AirflowException(f"SQL file not found: {path}")

    sql_text = path.read_text(encoding="utf-8")
    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            for statement in _split_sql(sql_text):
                print(f"Executing SQL from {filename}: {statement[:120]}...")
                cursor.execute(statement)


def check_csv_exists() -> None:
    """Fail fast if the local dataset is not mounted into Airflow."""
    if not DATA_PATH.exists():
        raise AirflowException(f"Dataset not found: {DATA_PATH}")
    if DATA_PATH.stat().st_size == 0:
        raise AirflowException(f"Dataset is empty: {DATA_PATH}")
    print(f"Found dataset: {DATA_PATH} ({DATA_PATH.stat().st_size:,} bytes)")


def wait_for_starrocks() -> None:
    """Wait until StarRocks query port is reachable and accepts a simple query."""
    deadline = time.monotonic() + 120
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((STARROCKS_HOST, STARROCKS_QUERY_PORT), timeout=5):
                pass
            with _starrocks_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    print(f"StarRocks is reachable at {STARROCKS_HOST}:{STARROCKS_QUERY_PORT}")
                    return
        except Exception as exc:
            last_error = exc
            print(f"Waiting for StarRocks: {exc}")
            time.sleep(5)

    raise AirflowException(f"StarRocks is not reachable after 120 seconds: {last_error}")


def create_starrocks_database() -> None:
    """Create the StarRocks database used by raw/dbt/mart tables."""
    _execute_sql_file("00_create_database.sql")


def create_raw_table() -> None:
    """Create the raw table with mostly VARCHAR columns for robust CSV loading."""
    _execute_sql_file("01_create_raw_table.sql")


def _local_csv_row_count() -> int:
    with DATA_PATH.open("r", encoding="utf-8-sig", newline="") as csv_file:
        return max(sum(1 for _ in csv.reader(csv_file)) - 1, 0)


def validate_raw_row_count() -> None:
    """Compare local CSV data rows with rows loaded into StarRocks raw table."""
    expected = _local_csv_row_count()

    with _starrocks_connection(STARROCKS_DATABASE) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM `{STARROCKS_RAW_TABLE}`")
            actual = int(cursor.fetchone()[0])

    print(f"Local CSV rows: {expected:,}")
    print(f"StarRocks raw rows: {actual:,}")

    if expected != actual:
        raise AirflowException(f"Raw row count mismatch: local_csv={expected}, starrocks_raw={actual}")


def log_mart_row_counts() -> None:
    """Log row counts for key fact and mart tables after dbt run/test."""
    with _starrocks_connection(STARROCKS_DATABASE) as connection:
        with connection.cursor() as cursor:
            for table_name in MART_TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = int(cursor.fetchone()[0])
                print(f"{table_name}: {row_count:,} rows")
                if row_count <= 0:
                    raise AirflowException(f"Table has no rows: {table_name}")


def _run_command(command: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    print(f"Running command: {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=merged_env,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise AirflowException(f"Command failed with exit code {result.returncode}: {' '.join(command)}")


def run_dbt_debug() -> None:
    _run_command(
        ["dbt", "debug", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROFILES_DIR)],
        cwd=DBT_PROJECT_DIR,
    )


def run_dbt_run() -> None:
    _run_command(
        [
            "dbt",
            "run",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--threads",
            os.environ.get("DBT_THREADS", "1"),
        ],
        cwd=DBT_PROJECT_DIR,
    )


def run_dbt_test() -> None:
    _run_command(
        [
            "dbt",
            "test",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--threads",
            os.environ.get("DBT_THREADS", "1"),
        ],
        cwd=DBT_PROJECT_DIR,
    )


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="hotel_booking_pipeline",
    description="Local batch pipeline: CSV -> MinIO -> StarRocks raw -> dbt marts.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hotel-booking", "local-mvp"],
) as dag:
    with TaskGroup(group_id="precheck") as precheck:
        # 1. Check that the local CSV is mounted in the Airflow container.
        check_csv_exists_task = PythonOperator(
            task_id="check_csv_exists",
            python_callable=check_csv_exists,
        )

        # 2. Run lightweight EDA and update docs/data_profile_summary.md.
        profile_dataset_task = BashOperator(
            task_id="profile_dataset",
            bash_command=f"python {SCRIPTS_DIR / 'profile_dataset.py'}",
        )

        check_csv_exists_task >> profile_dataset_task

    with TaskGroup(group_id="ingestion") as ingestion:
        # 3. Upload the CSV to MinIO raw storage.
        upload_csv_to_minio_task = BashOperator(
            task_id="upload_csv_to_minio",
            bash_command=f"python {SCRIPTS_DIR / 'upload_to_minio.py'}",
            env=SCRIPT_ENV,
            append_env=True,
        )

        # 4. Wait for StarRocks before running SQL/load/dbt tasks.
        wait_for_starrocks_task = PythonOperator(
            task_id="wait_for_starrocks",
            python_callable=wait_for_starrocks,
            retries=3,
            retry_delay=timedelta(seconds=30),
        )

        # 5. Create the StarRocks database if needed.
        create_starrocks_database_task = PythonOperator(
            task_id="create_starrocks_database",
            python_callable=create_starrocks_database,
        )

        # 6. Create the raw table if needed.
        create_raw_table_task = PythonOperator(
            task_id="create_raw_table",
            python_callable=create_raw_table,
        )

        # 7. Deterministic raw reload: upload is already done, script truncates raw before load.
        load_raw_to_starrocks_task = BashOperator(
            task_id="load_raw_to_starrocks",
            bash_command=f"python {SCRIPTS_DIR / 'load_raw_to_starrocks.py'} --skip-upload --method auto",
            env=SCRIPT_ENV,
            append_env=True,
        )

        # 8. Validate local CSV row count equals StarRocks raw row count.
        validate_raw_row_count_task = PythonOperator(
            task_id="validate_raw_row_count",
            python_callable=validate_raw_row_count,
        )

        (
            upload_csv_to_minio_task
            >> wait_for_starrocks_task
            >> create_starrocks_database_task
            >> create_raw_table_task
            >> load_raw_to_starrocks_task
            >> validate_raw_row_count_task
        )

    with TaskGroup(group_id="transformation") as transformation:
        # 9. Verify dbt profile and StarRocks adapter connectivity.
        dbt_debug_task = PythonOperator(
            task_id="dbt_debug",
            python_callable=run_dbt_debug,
        )

        # 10. Build staging, intermediate, dimension, fact, and mart tables in StarRocks.
        dbt_run_task = PythonOperator(
            task_id="dbt_run",
            python_callable=run_dbt_run,
        )

        # 11. Run dbt tests and fail the DAG if any test fails.
        dbt_test_task = PythonOperator(
            task_id="dbt_test",
            python_callable=run_dbt_test,
        )

        dbt_debug_task >> dbt_run_task >> dbt_test_task

    with TaskGroup(group_id="validation") as validation:
        # 12. Log mart row counts for Superset-facing tables.
        log_mart_row_counts_task = PythonOperator(
            task_id="log_mart_row_counts",
            python_callable=log_mart_row_counts,
        )

    precheck >> ingestion >> transformation >> validation
