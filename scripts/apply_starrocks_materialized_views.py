#!/usr/bin/env python3
"""Create, refresh, and validate StarRocks Materialized Views."""

from __future__ import annotations

import os
from pathlib import Path

try:
    import pymysql
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: PyMySQL. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"
SQL_DIR = ROOT_DIR / "starrocks" / "materialized_views"
EXPECTED_MATERIALIZED_VIEWS = {
    "mv_daily_booking_revenue",
    "mv_monthly_booking_revenue",
    "mv_hotel_performance",
}


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


def split_sql(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []

    remainder = "\n".join(current).strip()
    if remainder:
        statements.append(remainder)

    return statements


def connect() -> pymysql.Connection:
    return pymysql.connect(
        host=env("STARROCKS_EXTERNAL_HOST", env("STARROCKS_HOST", "127.0.0.1")),
        port=int(env("STARROCKS_EXTERNAL_QUERY_PORT", env("STARROCKS_QUERY_PORT", "9030"))),
        user=env("STARROCKS_USER", "root"),
        password=env("STARROCKS_PASSWORD", ""),
        autocommit=True,
        charset="utf8mb4",
    )


def execute_file(cursor: pymysql.cursors.Cursor, path: Path) -> None:
    print(f"\n==> Running {path.relative_to(ROOT_DIR)}")
    for statement in split_sql(path.read_text(encoding="utf-8")):
        preview = " ".join(statement.split())[:120]
        print(f"SQL: {preview}")
        cursor.execute(statement)
        if cursor.description:
            columns = [column[0] for column in cursor.description]
            print("\t".join(columns))
            for row in cursor.fetchall():
                print("\t".join("" if value is None else str(value) for value in row))


def validate_materialized_views(cursor: pymysql.cursors.Cursor) -> None:
    cursor.execute("SHOW MATERIALIZED VIEWS FROM hotel_booking")
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    by_name = {row["name"]: row for row in rows}

    missing = sorted(EXPECTED_MATERIALIZED_VIEWS - set(by_name))
    if missing:
        raise RuntimeError(f"Missing materialized views: {missing}")

    invalid: list[str] = []
    for name in sorted(EXPECTED_MATERIALIZED_VIEWS):
        row = by_name[name]
        is_active = str(row.get("is_active", "")).lower() == "true"
        query_rewrite_status = str(row.get("query_rewrite_status", ""))
        if not is_active or query_rewrite_status != "VALID":
            invalid.append(
                f"{name}: is_active={row.get('is_active')}, "
                f"query_rewrite_status={query_rewrite_status}"
            )

    if invalid:
        raise RuntimeError("Invalid materialized views: " + "; ".join(invalid))

    diff_checks = {
        "daily_total_bookings_diff": """
            SELECT
                (SELECT SUM(total_bookings) FROM hotel_booking.mv_daily_booking_revenue)
              - (SELECT SUM(total_bookings) FROM hotel_booking.mart_daily_booking_revenue)
        """,
        "monthly_total_bookings_diff": """
            SELECT
                (SELECT SUM(total_bookings) FROM hotel_booking.mv_monthly_booking_revenue)
              - (SELECT SUM(total_bookings) FROM hotel_booking.mart_monthly_booking_revenue)
        """,
        "hotel_bookings_diff": """
            SELECT
                (SELECT SUM(bookings) FROM hotel_booking.mv_hotel_performance)
              - (SELECT SUM(bookings) FROM hotel_booking.mart_hotel_performance)
        """,
    }

    for check_name, sql in diff_checks.items():
        cursor.execute(sql)
        diff_value = cursor.fetchone()[0]
        print(f"{check_name}: {diff_value}")
        if diff_value != 0:
            raise RuntimeError(f"Materialized View validation failed: {check_name}={diff_value}")


def main() -> int:
    load_env_file(ENV_PATH)
    sql_files = [
        SQL_DIR / "01_create_materialized_views.sql",
        SQL_DIR / "02_refresh_materialized_views.sql",
        SQL_DIR / "03_validate_materialized_views.sql",
    ]

    missing = [path for path in sql_files if not path.exists()]
    if missing:
        raise SystemExit(f"Missing SQL files: {missing}")

    with connect() as connection:
        with connection.cursor() as cursor:
            for path in sql_files:
                execute_file(cursor, path)
            validate_materialized_views(cursor)

    print("\nMaterialized View setup completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
