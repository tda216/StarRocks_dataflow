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
BATCH_DIR = AIRFLOW_HOME / "data" / "input" / "incremental_batches"
SCRIPTS_DIR = AIRFLOW_HOME / "scripts"
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", AIRFLOW_HOME / "dbt" / "hotel_booking"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR))

STARROCKS_HOST = os.environ.get("STARROCKS_HOST", "starrocks")
STARROCKS_QUERY_PORT = int(os.environ.get("STARROCKS_QUERY_PORT", "9030"))
STARROCKS_USER = os.environ.get("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.environ.get("STARROCKS_PASSWORD", "")
STARROCKS_DATABASE = os.environ.get("STARROCKS_DATABASE", "hotel_booking")

MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "hotel-booking-raw")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_WAREHOUSE_BUCKET = os.environ.get("MINIO_WAREHOUSE_BUCKET", "warehouse")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")

ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_DATABASE = os.environ.get("ICEBERG_DATABASE", "hotel_booking_lakehouse")
ICEBERG_RAW_HISTORY_TABLE = os.environ.get("ICEBERG_RAW_HISTORY_TABLE", "raw_hotel_bookings_history")

SCRIPT_ENV = {
    "MINIO_EXTERNAL_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ENDPOINT_URL": MINIO_ENDPOINT_URL,
    "MINIO_BUCKET": MINIO_BUCKET,
    "MINIO_BATCH_PREFIX": os.environ.get("MINIO_BATCH_PREFIX", "hotel_booking_demand/incremental_batches"),
    "MINIO_ROOT_USER": MINIO_ROOT_USER,
    "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
    "ICEBERG_REST_URI": ICEBERG_REST_URI,
    "ICEBERG_CATALOG_NAME": ICEBERG_CATALOG_NAME,
    "ICEBERG_DATABASE": ICEBERG_DATABASE,
    "ICEBERG_RAW_HISTORY_TABLE": ICEBERG_RAW_HISTORY_TABLE,
    "ICEBERG_WAREHOUSE": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD),
    "SPARK_MASTER": os.environ.get("SPARK_MASTER", "local[1]"),
    "SPARK_DRIVER_MEMORY": os.environ.get("SPARK_DRIVER_MEMORY", "768m"),
    "SPARK_EXECUTOR_MEMORY": os.environ.get("SPARK_EXECUTOR_MEMORY", "768m"),
    "SPARK_SQL_SHUFFLE_PARTITIONS": os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
}

VALIDATION_TABLES = [
    "scd_hotel_bookings",
    "int_current_hotel_bookings",
    "int_booking_metrics",
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


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _batch_row_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for batch_file in sorted(BATCH_DIR.glob("batch_*.csv")):
        with batch_file.open("r", encoding="utf-8", newline="") as csv_file:
            counts[batch_file.stem] = max(sum(1 for _ in csv.reader(csv_file)) - 1, 0)
    return counts


def check_csv_exists() -> None:
    """Fail fast if the local dataset is not mounted into Airflow."""
    if not DATA_PATH.exists():
        raise AirflowException(f"Dataset not found: {DATA_PATH}")
    if DATA_PATH.stat().st_size == 0:
        raise AirflowException(f"Dataset is empty: {DATA_PATH}")
    print(f"Found dataset: {DATA_PATH} ({DATA_PATH.stat().st_size:,} bytes)")


def wait_for_socket(name: str, host: str, port: int, timeout_seconds: int = 120) -> None:
    """Wait until a service host/port is reachable."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"{name} is reachable at {host}:{port}")
                return
        except Exception as exc:
            last_error = exc
            print(f"Waiting for {name}: {exc}")
            time.sleep(5)

    raise AirflowException(f"{name} is not reachable after {timeout_seconds} seconds: {last_error}")


def wait_for_iceberg_rest() -> None:
    wait_for_socket("Iceberg REST", "iceberg-rest", 8181)


def wait_for_minio() -> None:
    host, raw_port = MINIO_ENDPOINT.rsplit(":", 1)
    wait_for_socket("MinIO", host, int(raw_port))


def wait_for_starrocks() -> None:
    wait_for_socket("StarRocks", STARROCKS_HOST, STARROCKS_QUERY_PORT)
    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            print(f"StarRocks query check returned: {cursor.fetchone()[0]}")


def create_starrocks_database() -> None:
    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{STARROCKS_DATABASE}`")
    print(f"Ensured StarRocks database exists: {STARROCKS_DATABASE}")


def create_iceberg_external_catalog() -> None:
    """Create StarRocks external catalog that points to Iceberg REST + MinIO."""
    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW CATALOGS")
            existing_catalogs = {row[0] for row in cursor.fetchall()}
            if ICEBERG_CATALOG_NAME in existing_catalogs:
                print(f"External catalog already exists: {ICEBERG_CATALOG_NAME}")
                return

            sql = f"""
            CREATE EXTERNAL CATALOG `{ICEBERG_CATALOG_NAME}`
            COMMENT "External catalog to Apache Iceberg raw history on MinIO"
            PROPERTIES (
                "type" = "iceberg",
                "iceberg.catalog.type" = "rest",
                "iceberg.catalog.uri" = {_sql_string(ICEBERG_REST_URI)},
                "iceberg.catalog.warehouse" = {_sql_string(MINIO_WAREHOUSE_BUCKET)},
                "aws.s3.access_key" = {_sql_string(MINIO_ROOT_USER)},
                "aws.s3.secret_key" = {_sql_string(MINIO_ROOT_PASSWORD)},
                "aws.s3.endpoint" = {_sql_string(MINIO_ENDPOINT_URL)},
                "aws.s3.enable_path_style_access" = "true"
            )
            """
            print(f"Creating external catalog: {ICEBERG_CATALOG_NAME}")
            cursor.execute(sql)


def run_spark_iceberg_ingestion() -> None:
    _run_command(
        ["python", str(SCRIPTS_DIR / "ingest_batches_to_iceberg.py"), "--batch-dir", str(BATCH_DIR)],
        env=SCRIPT_ENV,
    )


def validate_iceberg_history_row_counts() -> None:
    """Compare generated batch CSV row counts with rows visible through StarRocks external catalog."""
    expected_counts = _batch_row_counts()
    if not expected_counts:
        raise AirflowException(f"No generated batch files found: {BATCH_DIR}")

    table_name = f"`{ICEBERG_CATALOG_NAME}`.`{ICEBERG_DATABASE}`.`{ICEBERG_RAW_HISTORY_TABLE}`"
    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT batch_id, COUNT(*) AS row_count
                FROM {table_name}
                GROUP BY batch_id
                """
            )
            actual_counts = {row[0]: int(row[1]) for row in cursor.fetchall()}

    print(f"Expected batch counts: {expected_counts}")
    print(f"Actual Iceberg counts: {actual_counts}")

    for batch_id, expected in expected_counts.items():
        actual = actual_counts.get(batch_id)
        if actual != expected:
            raise AirflowException(
                f"Iceberg row count mismatch for {batch_id}: expected={expected}, actual={actual}"
            )


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
            "--no-partial-parse",
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
            "--no-partial-parse",
            "--threads",
            os.environ.get("DBT_THREADS", "1"),
        ],
        cwd=DBT_PROJECT_DIR,
    )


def apply_starrocks_materialized_views() -> None:
    """Create, refresh, and validate StarRocks Materialized Views after dbt tests pass."""
    _run_command(["python", str(SCRIPTS_DIR / "apply_starrocks_materialized_views.py")])


def validate_materialized_view_rewrite() -> None:
    """Fail if StarRocks does not rewrite the daily revenue aggregate to its MV."""
    explain_sql = """
    EXPLAIN
    SELECT
        arrival_date,
        COUNT(*) AS total_bookings,
        SUM(is_cancelled) AS cancelled_bookings,
        COUNT(*) - SUM(is_cancelled) AS successful_bookings,
        SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
        SUM(total_nights) AS total_nights,
        SUM(estimated_revenue) AS estimated_revenue,
        SUM(realized_revenue) AS realized_revenue,
        AVG(adr) AS average_adr
    FROM hotel_booking.fact_bookings
    WHERE arrival_date IS NOT NULL
    GROUP BY arrival_date
    """

    with _starrocks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(explain_sql)
            explain_plan = "\n".join(str(row[0]) for row in cursor.fetchall())

    print("Materialized View rewrite EXPLAIN plan:")
    print(explain_plan)

    expected_markers = ["TABLE: mv_daily_booking_revenue", "MaterializedView: true"]
    missing_markers = [marker for marker in expected_markers if marker not in explain_plan]
    if missing_markers:
        raise AirflowException(
            "Materialized View rewrite validation failed. "
            f"Missing markers: {missing_markers}"
        )


def log_validation_counts() -> None:
    """Log SCD2/current/fact/mart counts and fail on empty serving tables."""
    with _starrocks_connection(STARROCKS_DATABASE) as connection:
        with connection.cursor() as cursor:
            for table_name in VALIDATION_TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = int(cursor.fetchone()[0])
                print(f"{table_name}: {row_count:,} rows")
                if row_count <= 0:
                    raise AirflowException(f"Table has no rows: {table_name}")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT booking_key
                    FROM scd_hotel_bookings
                    WHERE is_current = 1
                    GROUP BY booking_key
                    HAVING COUNT(*) > 1
                ) duplicate_current
                """
            )
            duplicate_current_count = int(cursor.fetchone()[0])
            print(f"booking_keys with duplicate current versions: {duplicate_current_count}")
            if duplicate_current_count > 0:
                raise AirflowException("SCD2 validation failed: duplicate current versions found")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT booking_key, batch_id
                    FROM stg_iceberg_raw_hotel_bookings
                    GROUP BY booking_key, batch_id
                    HAVING COUNT(DISTINCT record_hash) > 1
                ) multi_state_batch
                """
            )
            multi_state_batch_count = int(cursor.fetchone()[0])
            print(f"booking_key + batch_id groups with multiple record_hash values: {multi_state_batch_count}")
            if multi_state_batch_count > 0:
                raise AirflowException("Batch validation failed: multiple business states in one batch")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT a.booking_key
                    FROM scd_hotel_bookings a
                    JOIN scd_hotel_bookings b
                      ON a.booking_key = b.booking_key
                     AND a.valid_from < COALESCE(b.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
                     AND b.valid_from < COALESCE(a.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
                     AND a.valid_from <> b.valid_from
                    LIMIT 1
                ) overlap_check
                """
            )
            overlap_count = int(cursor.fetchone()[0])
            print(f"SCD2 overlapping period check rows: {overlap_count}")
            if overlap_count > 0:
                raise AirflowException("SCD2 validation failed: overlapping validity periods found")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="hotel_booking_pipeline",
    description="Local batch pipeline: CSV batches -> MinIO -> Iceberg -> StarRocks/dbt marts/MVs.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["hotel-booking", "iceberg", "scd2", "local-mvp"],
) as dag:
    with TaskGroup(group_id="precheck") as precheck:
        check_csv_exists_task = PythonOperator(
            task_id="check_csv_exists",
            python_callable=check_csv_exists,
        )

        profile_dataset_task = BashOperator(
            task_id="profile_dataset",
            bash_command=f"python {SCRIPTS_DIR / 'profile_dataset.py'}",
        )

        generate_batches_task = BashOperator(
            task_id="generate_synthetic_batches",
            bash_command=f"python {SCRIPTS_DIR / 'generate_synthetic_batches.py'}",
        )

        check_csv_exists_task >> profile_dataset_task >> generate_batches_task

    with TaskGroup(group_id="ingestion") as ingestion:
        wait_for_minio_task = PythonOperator(
            task_id="wait_for_minio",
            python_callable=wait_for_minio,
            retries=3,
            retry_delay=timedelta(seconds=30),
        )

        upload_batches_to_minio_task = BashOperator(
            task_id="upload_batches_to_minio",
            bash_command=f"python {SCRIPTS_DIR / 'upload_incremental_batches_to_minio.py'}",
            env=SCRIPT_ENV,
            append_env=True,
        )

        wait_for_iceberg_rest_task = PythonOperator(
            task_id="wait_for_iceberg_rest",
            python_callable=wait_for_iceberg_rest,
            retries=3,
            retry_delay=timedelta(seconds=30),
        )

        run_spark_iceberg_ingestion_task = PythonOperator(
            task_id="run_spark_iceberg_ingestion",
            python_callable=run_spark_iceberg_ingestion,
        )

        wait_for_starrocks_task = PythonOperator(
            task_id="wait_for_starrocks",
            python_callable=wait_for_starrocks,
            retries=3,
            retry_delay=timedelta(seconds=30),
        )

        create_starrocks_database_task = PythonOperator(
            task_id="create_starrocks_database",
            python_callable=create_starrocks_database,
        )

        create_iceberg_external_catalog_task = PythonOperator(
            task_id="create_iceberg_external_catalog",
            python_callable=create_iceberg_external_catalog,
        )

        validate_iceberg_history_task = PythonOperator(
            task_id="validate_iceberg_history_row_counts",
            python_callable=validate_iceberg_history_row_counts,
        )

        (
            wait_for_minio_task
            >> upload_batches_to_minio_task
            >> wait_for_iceberg_rest_task
            >> run_spark_iceberg_ingestion_task
            >> wait_for_starrocks_task
            >> create_starrocks_database_task
            >> create_iceberg_external_catalog_task
            >> validate_iceberg_history_task
        )

    with TaskGroup(group_id="transformation") as transformation:
        dbt_debug_task = PythonOperator(
            task_id="dbt_debug",
            python_callable=run_dbt_debug,
        )

        dbt_run_task = PythonOperator(
            task_id="dbt_run",
            python_callable=run_dbt_run,
        )

        dbt_test_task = PythonOperator(
            task_id="dbt_test",
            python_callable=run_dbt_test,
        )

        dbt_debug_task >> dbt_run_task >> dbt_test_task

    with TaskGroup(group_id="optimization") as optimization:
        apply_materialized_views_task = PythonOperator(
            task_id="apply_starrocks_materialized_views",
            python_callable=apply_starrocks_materialized_views,
        )

        validate_materialized_view_rewrite_task = PythonOperator(
            task_id="validate_materialized_view_rewrite",
            python_callable=validate_materialized_view_rewrite,
        )

        apply_materialized_views_task >> validate_materialized_view_rewrite_task

    with TaskGroup(group_id="validation") as validation:
        log_validation_counts_task = PythonOperator(
            task_id="log_validation_counts",
            python_callable=log_validation_counts,
        )

    precheck >> ingestion >> transformation >> optimization >> validation
