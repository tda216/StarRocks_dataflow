#!/usr/bin/env python3
"""Legacy direct StarRocks raw-table loader.

The primary MVP flow now uses generated CSV batches, Spark, Iceberg raw
history, StarRocks external catalog, and dbt SCD2/current models. Keep this
script only as a local fallback/demo for direct StarRocks FILES()/Stream Load.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    import pymysql
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: PyMySQL. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc

try:
    import requests
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: requests. Install with `python3 -m pip install -r scripts/requirements.txt`."
    ) from exc

from upload_to_minio import upload_file


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"
CSV_PATH = ROOT_DIR / "data" / "input" / "hotel_bookings.csv"
SQL_DIR = Path(os.environ.get("STARROCKS_SQL_DIR", ROOT_DIR / "airflow" / "include" / "sql"))
DATABASE_SQL = SQL_DIR / "00_create_database.sql"
TABLE_SQL = SQL_DIR / "01_create_raw_table.sql"
FILES_SQL = SQL_DIR / "02_load_raw_from_minio_files.sql"

SOURCE_COLUMNS = [
    "hotel",
    "is_canceled",
    "lead_time",
    "arrival_date_year",
    "arrival_date_month",
    "arrival_date_week_number",
    "arrival_date_day_of_month",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "adults",
    "children",
    "babies",
    "meal",
    "country",
    "market_segment",
    "distribution_channel",
    "is_repeated_guest",
    "previous_cancellations",
    "previous_bookings_not_canceled",
    "reserved_room_type",
    "assigned_room_type",
    "booking_changes",
    "deposit_type",
    "agent",
    "company",
    "days_in_waiting_list",
    "customer_type",
    "adr",
    "required_car_parking_spaces",
    "total_of_special_requests",
    "reservation_status",
    "reservation_status_date",
]


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


def starrocks_connection(database: str | None = None):
    return pymysql.connect(
        host=env("STARROCKS_EXTERNAL_HOST", "127.0.0.1"),
        port=int(env("STARROCKS_EXTERNAL_QUERY_PORT", env("STARROCKS_QUERY_PORT", "9030"))),
        user=env("STARROCKS_USER", "root"),
        password=env("STARROCKS_PASSWORD", ""),
        database=database,
        autocommit=True,
        charset="utf8mb4",
    )


def split_sql(sql_text: str) -> list[str]:
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


def execute_sql_file(path: Path, replacements: dict[str, str] | None = None) -> None:
    sql_text = path.read_text(encoding="utf-8")
    for key, value in (replacements or {}).items():
        sql_text = sql_text.replace("{{" + key + "}}", value.replace("'", "''"))

    with starrocks_connection() as connection:
        with connection.cursor() as cursor:
            for statement in split_sql(sql_text):
                cursor.execute(statement)


def local_csv_row_count() -> int:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {CSV_PATH}")

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        row_count = sum(1 for _ in reader)

    return max(row_count - 1, 0)


def raw_table_row_count() -> int:
    database = env("STARROCKS_DATABASE", "hotel_booking")
    table = env("STARROCKS_RAW_TABLE", "raw_hotel_bookings")

    with starrocks_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            return int(cursor.fetchone()[0])


def create_raw_objects() -> None:
    print("Creating StarRocks database and raw table...")
    execute_sql_file(DATABASE_SQL)
    execute_sql_file(TABLE_SQL)


def truncate_raw_table() -> None:
    database = env("STARROCKS_DATABASE", "hotel_booking")
    table = env("STARROCKS_RAW_TABLE", "raw_hotel_bookings")

    with starrocks_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table}`")


def load_with_files() -> None:
    replacements = {
        "MINIO_BUCKET": env("MINIO_BUCKET", "hotel-booking-raw"),
        "MINIO_RAW_OBJECT_KEY": env("MINIO_RAW_OBJECT_KEY", "hotel_booking_demand/hotel_bookings.csv"),
        "MINIO_ENDPOINT": env("MINIO_ENDPOINT", "minio:9000"),
        "MINIO_ROOT_USER": env("MINIO_ROOT_USER", "minioadmin"),
        "MINIO_ROOT_PASSWORD": env("MINIO_ROOT_PASSWORD", "minioadmin"),
    }
    print("Loading raw table with StarRocks INSERT ... FILES() from MinIO...")
    execute_sql_file(FILES_SQL, replacements)


def make_stream_load_file() -> Path:
    source_file = f"s3://{env('MINIO_BUCKET', 'hotel-booking-raw')}/{env('MINIO_RAW_OBJECT_KEY', 'hotel_booking_demand/hotel_bookings.csv')}"
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".csv",
        prefix="hotel_bookings_stream_load_",
        delete=False,
    )
    temp_path = Path(temp_file.name)

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as input_file, temp_file:
        reader = csv.DictReader(input_file)
        writer = csv.writer(temp_file)
        missing_columns = [column for column in SOURCE_COLUMNS if column not in (reader.fieldnames or [])]
        if missing_columns:
            raise ValueError(f"CSV missing expected columns: {missing_columns}")

        for row in reader:
            writer.writerow([row.get(column, "") for column in SOURCE_COLUMNS] + [source_file, loaded_at])

    return temp_path


def stream_load_file(path: Path) -> None:
    database = env("STARROCKS_DATABASE", "hotel_booking")
    table = env("STARROCKS_RAW_TABLE", "raw_hotel_bookings")
    host = env("STARROCKS_EXTERNAL_HOST", "127.0.0.1")
    http_port = env("STARROCKS_EXTERNAL_HTTP_PORT", env("STARROCKS_HTTP_PORT", "8030"))
    user = env("STARROCKS_USER", "root")
    password = env("STARROCKS_PASSWORD", "")
    label = f"hotel_bookings_raw_{uuid.uuid4().hex}"
    url = f"http://{host}:{http_port}/api/{database}/{table}/_stream_load"

    headers = {
        "label": label,
        "format": "CSV",
        "column_separator": ",",
        "row_delimiter": "\\n",
        "enclose": '"',
        "columns": ",".join(SOURCE_COLUMNS + ["source_file", "loaded_at"]),
        "max_filter_ratio": "0",
        "Expect": "100-continue",
    }

    print(f"Loading raw table with Python Stream Load fallback, label={label}...")
    with path.open("rb") as load_file:
        response = requests.put(
            url,
            data=load_file,
            headers=headers,
            auth=(user, password),
            timeout=600,
            allow_redirects=True,
        )

    response.raise_for_status()
    payload = response.json()
    status = str(payload.get("Status", "")).lower()
    if status not in {"success", "publish timeout"}:
        raise RuntimeError(f"Stream Load failed: {json.dumps(payload, indent=2)}")
    print(json.dumps(payload, indent=2))


def load_with_stream() -> None:
    temp_path = make_stream_load_file()
    try:
        stream_load_file(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)


def validate_counts() -> None:
    expected = local_csv_row_count()
    actual = raw_table_row_count()
    print(f"Local CSV rows: {expected:,}")
    print(f"StarRocks raw rows: {actual:,}")
    if expected != actual:
        raise RuntimeError(f"Row count mismatch: local_csv={expected}, raw_table={actual}")


def print_sample_rows() -> None:
    database = env("STARROCKS_DATABASE", "hotel_booking")
    table = env("STARROCKS_RAW_TABLE", "raw_hotel_bookings")

    with starrocks_connection(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT hotel, is_canceled, arrival_date_year, country, adr, source_file, loaded_at
                FROM `{table}`
                LIMIT 5
                """
            )
            rows = cursor.fetchall()

    print("Sample rows:")
    for row in rows:
        print(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legacy load of hotel_bookings.csv into StarRocks raw table.")
    parser.add_argument(
        "--method",
        choices=["auto", "files", "stream"],
        default="auto",
        help="Load method. auto tries FILES() first, then Stream Load fallback.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip MinIO upload and only load/validate StarRocks.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append rows instead of truncating raw_hotel_bookings before load.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(ENV_PATH)

    try:
        if not args.skip_upload:
            upload_file()

        create_raw_objects()
        if not args.append:
            truncate_raw_table()

        if args.method == "files":
            load_with_files()
        elif args.method == "stream":
            load_with_stream()
        else:
            try:
                load_with_files()
            except Exception as exc:
                print(f"FILES() load failed, using Stream Load fallback. Reason: {exc}")
                truncate_raw_table()
                load_with_stream()

        validate_counts()
        print_sample_rows()
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
