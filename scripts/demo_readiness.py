#!/usr/bin/env python3
"""Run read-only demo checks for the local StarRocks BI POC."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from minio import Minio
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: minio. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc

try:
    import pymysql
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: PyMySQL. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"

EXPECTED_BATCH_IDS = {
    "batch_001_initial",
    "batch_002_updates",
    "batch_003_duplicate_replay",
    "batch_004_same_state",
    "batch_005_reverted_state",
}

EXPECTED_STARROCKS_OBJECTS = [
    "stg_iceberg_raw_hotel_bookings",
    "int_hotel_bookings_deduped",
    "int_current_hotel_bookings",
    "int_booking_metrics",
    "fact_bookings",
    "dim_date",
    "dim_hotel",
    "dim_room",
    "dim_market_segment",
    "dim_channel",
    "dim_country",
    "dim_customer_type",
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

EXPECTED_MATERIALIZED_VIEWS = {
    "mv_daily_booking_revenue",
    "mv_monthly_booking_revenue",
    "mv_hotel_performance",
}

EXPECTED_SILVER_TABLES = [
    "deduped_hotel_bookings",
    "current_hotel_bookings",
    "booking_metrics",
]


class DemoReport:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def section(self, title: str) -> None:
        print(f"\n== {title} ==")

    def ok(self, message: str) -> None:
        print(f"[OK] {message}")

    def warn(self, message: str) -> None:
        self.warnings.append(message)
        print(f"[WARN] {message}")

    def fail(self, message: str) -> None:
        self.errors.append(message)
        print(f"[FAIL] {message}")

    def finish(self) -> int:
        self.section("Summary")
        if self.warnings:
            print("Warnings:")
            for warning in self.warnings:
                print(f"- {warning}")
        if self.errors:
            print("Errors:")
            for error in self.errors:
                print(f"- {error}")
            return 1
        print("Demo readiness checks passed.")
        return 0


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def bool_env(name: str, default: str = "false") -> bool:
    return env(name, default).lower() in {"1", "true", "yes", "y"}


def minio_client() -> Minio:
    endpoint = env("MINIO_EXTERNAL_ENDPOINT", env("MINIO_ENDPOINT", "localhost:9000"))
    return Minio(
        endpoint,
        access_key=env("MINIO_ROOT_USER", "minioadmin"),
        secret_key=env("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=bool_env("MINIO_SECURE", "false"),
    )


def starrocks_connection(database: str | None = None) -> pymysql.Connection:
    return pymysql.connect(
        host=env("STARROCKS_EXTERNAL_HOST", env("STARROCKS_HOST", "127.0.0.1")),
        port=int(env("STARROCKS_EXTERNAL_QUERY_PORT", env("STARROCKS_QUERY_PORT", "9030"))),
        user=env("STARROCKS_USER", "root"),
        password=env("STARROCKS_PASSWORD", ""),
        database=database,
        autocommit=True,
        charset="utf8mb4",
    )


def fetch_scalar(cursor: pymysql.cursors.Cursor, sql: str) -> Any:
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row else None


def print_rows(columns: list[str], rows: list[tuple[Any, ...]], limit: int | None = None) -> None:
    if limit is not None:
        rows = rows[:limit]
    print("\t".join(columns))
    for row in rows:
        print("\t".join("" if value is None else str(value) for value in row))


def check_dataset(report: DemoReport) -> None:
    report.section("Dataset")
    dataset_path = ROOT_DIR / "data" / "input" / "hotel_bookings.csv"
    if not dataset_path.exists():
        report.fail(f"Missing dataset: {dataset_path}")
        return
    report.ok(f"Dataset exists: {dataset_path.relative_to(ROOT_DIR)}")

    batch_dir = ROOT_DIR / "data" / "input" / "incremental_batches"
    batch_files = sorted(batch_dir.glob("batch_*.csv"))
    if not batch_files:
        report.fail(f"No generated batch files found: {batch_dir.relative_to(ROOT_DIR)}")
        return
    report.ok(f"Generated batch files: {len(batch_files)}")
    for batch_file in batch_files:
        print(f"- {batch_file.relative_to(ROOT_DIR)}")


def check_minio(report: DemoReport) -> None:
    report.section("MinIO Raw Storage")
    bucket = env("MINIO_BUCKET", "hotel-booking-raw")
    prefix = env("MINIO_BATCH_PREFIX", "hotel_booking_demand/incremental_batches").strip("/")

    try:
        client = minio_client()
        if not client.bucket_exists(bucket):
            report.fail(f"MinIO bucket does not exist: {bucket}")
            return
        report.ok(f"Bucket exists: {bucket}")

        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        batch_objects = [obj for obj in objects if Path(obj.object_name).name.startswith("batch_")]
        if not batch_objects:
            report.fail(f"No batch objects found in s3://{bucket}/{prefix}/")
            return

        partitioned_objects = [
            obj
            for obj in batch_objects
            if all(
                marker in obj.object_name
                for marker in ("etl_year=", "etl_month=", "etl_day=", "raw_batch_sequence=")
            )
        ]
        legacy_objects = [obj for obj in batch_objects if obj not in partitioned_objects]
        if partitioned_objects:
            report.ok(f"Partitioned raw batch objects found: {len(partitioned_objects)}")
            for obj in partitioned_objects:
                print(f"- s3://{bucket}/{obj.object_name} ({obj.size} bytes)")
        else:
            report.fail("No partitioned raw batch objects found under MinIO raw prefix")

        if legacy_objects:
            print(f"Legacy flat batch objects also present from earlier runs: {len(legacy_objects)}")

        warehouse_bucket = env("MINIO_WAREHOUSE_BUCKET", "warehouse")
        if client.bucket_exists(warehouse_bucket):
            report.ok(f"Iceberg warehouse bucket exists: {warehouse_bucket}")
        else:
            report.fail(f"Iceberg warehouse bucket does not exist: {warehouse_bucket}")
    except Exception as exc:
        report.fail(f"MinIO check failed: {exc}")


def check_starrocks_core(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("StarRocks Core")
    result = fetch_scalar(cursor, "SELECT 1")
    report.ok(f"StarRocks query check returned: {result}")

    cursor.execute("SHOW DATABASES")
    databases = {row[0] for row in cursor.fetchall()}
    database = env("STARROCKS_DATABASE", "hotel_booking")
    if database in databases:
        report.ok(f"Database exists: {database}")
    else:
        report.fail(f"Missing database: {database}")

    cursor.execute("SHOW CATALOGS")
    catalogs = {row[0] for row in cursor.fetchall()}
    catalog_name = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    if catalog_name in catalogs:
        report.ok(f"External catalog exists: {catalog_name}")
    else:
        report.fail(f"Missing external catalog: {catalog_name}")


def check_iceberg_history(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("Iceberg Raw History")
    catalog = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    database = env("ICEBERG_DATABASE", "hotel_booking_lakehouse")
    table = env("ICEBERG_RAW_HISTORY_TABLE", "raw_hotel_bookings_history")
    full_name = f"`{catalog}`.`{database}`.`{table}`"

    try:
        cursor.execute(f"SHOW DATABASES FROM `{catalog}`")
        databases = {row[0] for row in cursor.fetchall()}
        if database in databases:
            report.ok(f"Iceberg database visible from StarRocks: {catalog}.{database}")
        else:
            report.fail(f"Missing Iceberg database from StarRocks: {catalog}.{database}")

        cursor.execute(f"SHOW TABLES FROM `{catalog}`.`{database}`")
        tables = {row[0] for row in cursor.fetchall()}
        if table in tables:
            report.ok(f"Iceberg table visible from StarRocks: {catalog}.{database}.{table}")
        else:
            report.fail(f"Missing Iceberg table from StarRocks: {catalog}.{database}.{table}")

        cursor.execute(
            f"""
            SELECT batch_id, COUNT(*) AS row_count
            FROM {full_name}
            GROUP BY batch_id
            ORDER BY batch_id
            """
        )
        rows = cursor.fetchall()
        if not rows:
            report.fail("Iceberg raw history has no rows")
            return
        print_rows(["batch_id", "row_count"], rows)

        actual_batch_ids = {row[0] for row in rows}
        missing_batches = sorted(EXPECTED_BATCH_IDS - actual_batch_ids)
        if missing_batches:
            report.fail(f"Missing expected batch ids in Iceberg history: {missing_batches}")
        else:
            report.ok("All expected synthetic batch ids are present")
    except Exception as exc:
        report.fail(f"Iceberg history check failed: {exc}")


def check_iceberg_silver_tables(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("Iceberg Silver Tables")
    catalog = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    database = env("ICEBERG_SILVER_DATABASE", "hotel_booking_silver")
    expected_tables = [
        env("ICEBERG_SILVER_DEDUPED_TABLE", EXPECTED_SILVER_TABLES[0]),
        env("ICEBERG_SILVER_CURRENT_TABLE", EXPECTED_SILVER_TABLES[1]),
        env("ICEBERG_SILVER_METRICS_TABLE", EXPECTED_SILVER_TABLES[2]),
    ]

    try:
        cursor.execute(f"SHOW DATABASES FROM `{catalog}`")
        databases = {row[0] for row in cursor.fetchall()}
        if database in databases:
            report.ok(f"Silver Iceberg database visible from StarRocks: {catalog}.{database}")
        else:
            report.fail(f"Missing Silver Iceberg database from StarRocks: {catalog}.{database}")
            return

        cursor.execute(f"SHOW TABLES FROM `{catalog}`.`{database}`")
        tables = {row[0] for row in cursor.fetchall()}
        missing = sorted(set(expected_tables) - tables)
        if missing:
            report.fail(f"Missing Silver Iceberg tables: {missing}")
            return

        report.ok("All expected Silver Iceberg tables exist")
        rows: list[tuple[str, int]] = []
        for table_name in expected_tables:
            row_count = int(
                fetch_scalar(
                    cursor,
                    f"SELECT COUNT(*) FROM `{catalog}`.`{database}`.`{table_name}`",
                )
            )
            rows.append((table_name, row_count))
            if row_count <= 0:
                report.fail(f"Silver Iceberg table has no rows: {table_name}")
        print_rows(["table_name", "row_count"], rows)
    except Exception as exc:
        report.fail(f"Silver Iceberg check failed: {exc}")


def check_dbt_objects(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("dbt Views and Serving Tables")
    database = env("STARROCKS_DATABASE", "hotel_booking")

    cursor.execute(f"SHOW TABLES FROM `{database}`")
    tables = {row[0] for row in cursor.fetchall()}
    missing_tables = sorted(set(EXPECTED_STARROCKS_OBJECTS) - tables)
    if missing_tables:
        report.fail(f"Missing dbt views/tables: {missing_tables}")
    else:
        report.ok("All expected staging/intermediate views and fact/dim/mart tables exist")

    count_rows: list[tuple[str, int]] = []
    for table_name in EXPECTED_STARROCKS_OBJECTS:
        if table_name not in tables:
            continue
        row_count = int(fetch_scalar(cursor, f"SELECT COUNT(*) FROM `{database}`.`{table_name}`"))
        count_rows.append((table_name, row_count))
        if row_count <= 0:
            report.fail(f"dbt object has no rows: {table_name}")

    print_rows(["table_name", "row_count"], count_rows)


def check_current_state_validations(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("Dedup and Current-State Validation")
    database = env("STARROCKS_DATABASE", "hotel_booking")

    checks = {
        "multiple_states_per_booking_batch": f"""
            SELECT COUNT(*)
            FROM (
                SELECT booking_key, batch_id
                FROM `{database}`.`stg_iceberg_raw_hotel_bookings`
                GROUP BY booking_key, batch_id
                HAVING COUNT(DISTINCT record_hash) > 1
            ) invalid_batches
        """,
        "current_rows_vs_distinct_booking_keys": f"""
            SELECT ABS(
                (SELECT COUNT(*) FROM `{database}`.`int_current_hotel_bookings`)
              - (SELECT COUNT(DISTINCT booking_key) FROM `{database}`.`stg_iceberg_raw_hotel_bookings`)
            )
        """,
    }

    for check_name, sql in checks.items():
        value = int(fetch_scalar(cursor, sql))
        print(f"{check_name}: {value}")
        if value != 0:
            report.fail(f"{check_name} should be 0, got {value}")

    cursor.execute(
        f"""
        SELECT booking_key, first_seen_batch_id
        FROM `{database}`.`int_current_hotel_bookings`
        WHERE booking_key IN ('hotel_booking_demand:1', 'hotel_booking_demand:2')
        ORDER BY booking_key
        """
    )
    rows = cursor.fetchall()
    print_rows(["booking_key", "first_seen_batch_id"], rows)
    fixture_batches = {row[0]: row[1] for row in rows}
    expected_fixture_batches = {
        "hotel_booking_demand:1": "batch_001_initial",
        "hotel_booking_demand:2": "batch_005_reverted_state",
    }
    for booking_key, expected_batch in expected_fixture_batches.items():
        actual_batch = fixture_batches.get(booking_key)
        if actual_batch != expected_batch:
            report.fail(
                f"Current-state fixture mismatch for {booking_key}: expected={expected_batch}, actual={actual_batch}"
            )


def check_materialized_views(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("StarRocks Materialized Views")
    database = env("STARROCKS_DATABASE", "hotel_booking")

    cursor.execute(f"SHOW MATERIALIZED VIEWS FROM `{database}`")
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    by_name = {dict(zip(columns, row))["name"]: dict(zip(columns, row)) for row in rows}

    missing = sorted(EXPECTED_MATERIALIZED_VIEWS - set(by_name))
    if missing:
        report.fail(f"Missing materialized views: {missing}")
    else:
        report.ok("All expected Materialized Views exist")

    display_rows: list[tuple[str, str, str]] = []
    for mv_name in sorted(EXPECTED_MATERIALIZED_VIEWS):
        row = by_name.get(mv_name)
        if not row:
            continue
        is_active = str(row.get("is_active", ""))
        query_rewrite_status = str(row.get("query_rewrite_status", ""))
        display_rows.append((mv_name, is_active, query_rewrite_status))
        if is_active.lower() != "true":
            report.fail(f"{mv_name} is not active: {is_active}")
        if query_rewrite_status != "VALID":
            report.fail(f"{mv_name} query rewrite status is not VALID: {query_rewrite_status}")
    print_rows(["name", "is_active", "query_rewrite_status"], display_rows)

    diff_checks = {
        "daily_total_bookings_diff": f"""
            SELECT
                (SELECT SUM(total_bookings) FROM `{database}`.`mv_daily_booking_revenue`)
              - (SELECT SUM(total_bookings) FROM `{database}`.`mart_daily_booking_revenue`)
        """,
        "monthly_total_bookings_diff": f"""
            SELECT
                (SELECT SUM(total_bookings) FROM `{database}`.`mv_monthly_booking_revenue`)
              - (SELECT SUM(total_bookings) FROM `{database}`.`mart_monthly_booking_revenue`)
        """,
        "hotel_bookings_diff": f"""
            SELECT
                (SELECT SUM(bookings) FROM `{database}`.`mv_hotel_performance`)
              - (SELECT SUM(bookings) FROM `{database}`.`mart_hotel_performance`)
        """,
    }

    for check_name, sql in diff_checks.items():
        value = fetch_scalar(cursor, sql)
        print(f"{check_name}: {value}")
        if value != 0:
            report.fail(f"{check_name} should be 0, got {value}")


def check_query_rewrite(report: DemoReport, cursor: pymysql.cursors.Cursor) -> None:
    report.section("Materialized View Query Rewrite Demo")
    cursor.execute(
        """
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
    )
    explain_plan = "\n".join(str(row[0]) for row in cursor.fetchall())
    important_lines = [
        line
        for line in explain_plan.splitlines()
        if "mv_daily_booking_revenue" in line or "MaterializedView" in line
    ]
    if important_lines:
        for line in important_lines:
            print(line)
    else:
        print(explain_plan)

    expected_markers = ["TABLE: mv_daily_booking_revenue", "MaterializedView: true"]
    missing_markers = [marker for marker in expected_markers if marker not in explain_plan]
    if missing_markers:
        report.fail(f"Query rewrite did not use mv_daily_booking_revenue. Missing markers: {missing_markers}")
    else:
        report.ok("StarRocks rewrites the matching aggregate query to mv_daily_booking_revenue")


def main() -> int:
    load_env_file(ENV_PATH)
    report = DemoReport()

    check_dataset(report)
    check_minio(report)

    try:
        with starrocks_connection() as connection:
            with connection.cursor() as cursor:
                check_starrocks_core(report, cursor)
                check_iceberg_history(report, cursor)
                check_iceberg_silver_tables(report, cursor)
                check_dbt_objects(report, cursor)
                check_current_state_validations(report, cursor)
                check_materialized_views(report, cursor)
                check_query_rewrite(report, cursor)
    except Exception as exc:
        report.fail(f"StarRocks checks failed: {exc}")

    return report.finish()


if __name__ == "__main__":
    raise SystemExit(main())
