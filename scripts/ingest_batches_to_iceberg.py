#!/usr/bin/env python3
"""Append generated batch CSV files from MinIO to an Iceberg raw history table."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from urllib.parse import quote

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pyspark. Run this inside the Airflow image or install PySpark locally."
    ) from exc

from batch_storage import BatchStorageMetadata, build_partitioned_batch_object_key


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BATCH_DIR = ROOT_DIR / "data" / "input" / "incremental_batches"

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

BATCH_COLUMNS = [
    "source_dataset",
    "original_source_row_number",
    "booking_key",
    "batch_id",
    "batch_sequence",
    "batch_effective_at",
    "batch_row_number",
    "synthetic_operation",
]

OUTPUT_COLUMNS = [
    "source_dataset",
    "original_source_row_number",
    "booking_key",
    "batch_id",
    "batch_sequence",
    "batch_effective_at",
    "batch_row_number",
    "etl_year",
    "etl_month",
    "etl_day",
    "etl_date",
    "raw_batch_sequence",
    "source_file_name",
    "source_object_path",
    "file_hash",
    "ingested_at",
    "row_ingestion_id",
    "synthetic_operation",
    *SOURCE_COLUMNS,
]

BRONZE_PARTITION_COLUMNS = {
    "etl_year": "INT",
    "etl_month": "INT",
    "etl_day": "INT",
    "etl_date": "STRING",
    "raw_batch_sequence": "STRING",
}

BRONZE_PARTITION_FIELD = "etl_date"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_spark() -> SparkSession:
    catalog_name = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    rest_uri = env("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = env("ICEBERG_WAREHOUSE", "s3://warehouse/")
    minio_endpoint = env("MINIO_ENDPOINT_URL", f"http://{env('MINIO_ENDPOINT', 'minio:9000')}")
    access_key = env("MINIO_ROOT_USER", "minioadmin")
    secret_key = env("MINIO_ROOT_PASSWORD", "minioadmin")
    aws_region = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"))
    os.environ.setdefault("AWS_REGION", aws_region)
    os.environ.setdefault("AWS_DEFAULT_REGION", aws_region)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", env("AWS_ACCESS_KEY_ID", access_key))
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", env("AWS_SECRET_ACCESS_KEY", secret_key))
    java_options = (
        f"-Daws.region={aws_region} "
        f"-Daws.accessKeyId={os.environ['AWS_ACCESS_KEY_ID']} "
        f"-Daws.secretAccessKey={os.environ['AWS_SECRET_ACCESS_KEY']}"
    )
    packages = env(
        "SPARK_ICEBERG_PACKAGES",
        ",".join(
            [
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
            ]
        ),
    )

    return (
        SparkSession.builder.appName("hotel-booking-iceberg-ingestion")
        .master(env("SPARK_MASTER", "local[1]"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", rest_uri)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog_name}.cache-enabled", "false")
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog_name}.s3.region", aws_region)
        .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(f"spark.sql.catalog.{catalog_name}.client.region", aws_region)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.sql.shuffle.partitions", env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "768m"))
        .config("spark.executor.memory", env("SPARK_EXECUTOR_MEMORY", "768m"))
        .getOrCreate()
    )


def create_table_if_needed(spark: SparkSession, table_name: str) -> None:
    catalog, database, _ = table_name.split(".", 2)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

    source_column_ddl = ",\n        ".join(f"{column} STRING" for column in SOURCE_COLUMNS)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        source_dataset STRING,
        original_source_row_number BIGINT,
        booking_key STRING,
        batch_id STRING,
        batch_sequence INT,
        batch_effective_at TIMESTAMP,
        batch_row_number BIGINT,
        etl_year INT,
        etl_month INT,
        etl_day INT,
        etl_date STRING,
        raw_batch_sequence STRING,
        source_file_name STRING,
        source_object_path STRING,
        file_hash STRING,
        ingested_at TIMESTAMP,
        row_ingestion_id STRING,
        synthetic_operation STRING,
        {source_column_ddl}
    )
    USING iceberg
    PARTITIONED BY (etl_date)
    """

    try:
        spark.sql(create_sql)
    except Exception as exc:
        if _is_corrupt_metadata_error(exc):
            print(
                f"Detected corrupt Iceberg metadata for {table_name}. "
                "Dropping the catalog entry and recreating the deterministic local MVP table."
            )
            drop_corrupt_iceberg_table(spark, table_name)
            spark.sql(create_sql)
        else:
            raise
    ensure_table_columns(spark, table_name)
    ensure_etl_partition_spec(spark, table_name)
    drop_legacy_table_columns(spark, table_name)


def ensure_table_columns(spark: SparkSession, table_name: str) -> None:
    existing_columns = {
        row["col_name"]
        for row in spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        if row["col_name"] and not str(row["col_name"]).startswith("#")
    }
    for column_name, column_type in BRONZE_PARTITION_COLUMNS.items():
        if column_name not in existing_columns:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            print(f"Added missing Bronze column: {table_name}.{column_name} {column_type}")


def _is_duplicate_partition_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in ("already exists", "duplicate", "cannot add", "exists in partition spec"))


def _is_missing_partition_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in ("cannot find", "not found", "missing", "does not exist"))


def _is_corrupt_metadata_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "metadata.json" in message
        or "location does not exist" in message
        or "nosuchtableexception" in message
    )


def drop_corrupt_iceberg_table(spark: SparkSession, table_name: str) -> None:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as exc:
        print(f"Spark DROP TABLE could not clean corrupt Iceberg entry for {table_name}: {exc}")
    unregister_iceberg_table_via_rest(table_name)


def unregister_iceberg_table_via_rest(table_name: str) -> None:
    try:
        import requests
    except ImportError:
        print("requests is unavailable; skipping REST catalog unregister fallback")
        return

    parts = table_name.split(".")
    if len(parts) != 3:
        print(f"Cannot unregister table with unexpected identifier format: {table_name}")
        return

    _, namespace, table = parts
    rest_uri = env("ICEBERG_REST_URI", "http://iceberg-rest:8181").rstrip("/")
    url = f"{rest_uri}/v1/namespaces/{quote(namespace, safe='')}/tables/{quote(table, safe='')}"
    try:
        response = requests.delete(url, timeout=20)
    except Exception as exc:
        print(f"REST catalog unregister request failed for {table_name}: {exc}")
        return

    if response.status_code in {200, 202, 204, 404}:
        print(f"REST catalog unregister status for {table_name}: {response.status_code}")
        return

    print(
        f"REST catalog unregister returned {response.status_code} for {table_name}: "
        f"{response.text[:500]}"
    )


def ensure_etl_partition_spec(spark: SparkSession, table_name: str) -> None:
    try:
        spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD {BRONZE_PARTITION_FIELD}")
        print(f"Added Bronze Iceberg partition field: {table_name}.{BRONZE_PARTITION_FIELD}")
    except Exception as exc:
        if _is_duplicate_partition_error(exc):
            print(f"Bronze Iceberg partition field already present: {BRONZE_PARTITION_FIELD}")
        else:
            raise

    # Existing local MVP tables may have been created with older partition specs.
    # Drop legacy fields from the current spec so future writes use etl_date.
    for legacy_partition_field in ("batch_id", "watermark_date"):
        try:
            spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD {legacy_partition_field}")
            print(f"Dropped legacy Bronze Iceberg partition field: {table_name}.{legacy_partition_field}")
        except Exception as exc:
            if _is_missing_partition_error(exc):
                print(f"Legacy Bronze Iceberg partition field already absent: {legacy_partition_field}")
            else:
                print(
                    f"Could not drop legacy partition field {legacy_partition_field}; "
                    f"existing data remains queryable: {exc}"
                )


def drop_legacy_table_columns(spark: SparkSession, table_name: str) -> None:
    existing_columns = {
        row["col_name"]
        for row in spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        if row["col_name"] and not str(row["col_name"]).startswith("#")
    }

    for legacy_column in ("record_hash", "watermark_date"):
        if legacy_column not in existing_columns:
            continue
        try:
            spark.sql(f"ALTER TABLE {table_name} DROP COLUMN {legacy_column}")
            print(f"Dropped legacy Bronze Iceberg column: {table_name}.{legacy_column}")
        except Exception as exc:
            print(
                f"Could not drop legacy Bronze column {legacy_column}; "
                f"existing table may require a clean rebuild if append fails: {exc}"
            )


def batch_already_ingested(spark: SparkSession, table_name: str, batch_id: str) -> bool:
    count = spark.sql(
        f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE batch_id = '{batch_id}'"
    ).collect()[0]["row_count"]
    return int(count) > 0


def read_and_enrich_batch(
    spark: SparkSession,
    *,
    local_batch_file: Path,
    object_key: str,
    bucket: str,
    storage_metadata: BatchStorageMetadata,
) :
    schema = StructType([StructField(column, StringType(), True) for column in [*BATCH_COLUMNS, *SOURCE_COLUMNS]])
    s3a_uri = f"s3a://{bucket}/{object_key}"
    source_object_path = f"s3://{bucket}/{object_key}"
    file_hash = sha256_file(local_batch_file)

    df = spark.read.option("header", "true").schema(schema).csv(s3a_uri)

    df = (
        df.withColumn("original_source_row_number", F.col("original_source_row_number").cast("bigint"))
        .withColumn("batch_sequence", F.col("batch_sequence").cast("int"))
        .withColumn("batch_effective_at", F.to_timestamp(F.col("batch_effective_at")))
        .withColumn("batch_row_number", F.col("batch_row_number").cast("bigint"))
        .withColumn("etl_year", F.lit(storage_metadata.etl_year).cast("int"))
        .withColumn("etl_month", F.lit(storage_metadata.etl_month).cast("int"))
        .withColumn("etl_day", F.lit(storage_metadata.etl_day).cast("int"))
        .withColumn("etl_date", F.lit(storage_metadata.etl_date))
        .withColumn("raw_batch_sequence", F.lit(storage_metadata.raw_batch_sequence))
        .withColumn("source_file_name", F.lit(local_batch_file.name))
        .withColumn("source_object_path", F.lit(source_object_path))
        .withColumn("file_hash", F.lit(file_hash))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "row_ingestion_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(source_object_path),
                    F.col("booking_key"),
                    F.col("batch_id"),
                    F.col("batch_row_number").cast("string"),
                ),
                256,
            ),
        )
    )
    return df.select(*OUTPUT_COLUMNS)


def ingest_batches(batch_dir: Path, force: bool = False) -> None:
    batch_files = sorted(batch_dir.glob("batch_*.csv"))
    if not batch_files:
        raise FileNotFoundError(f"No batch_*.csv files found in {batch_dir}")

    catalog_name = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    database = env("ICEBERG_DATABASE", "hotel_booking_lakehouse")
    table = env("ICEBERG_RAW_HISTORY_TABLE", "raw_hotel_bookings_history")
    table_name = f"{catalog_name}.{database}.{table}"
    bucket = env("MINIO_BUCKET", "hotel-booking-raw")
    prefix = env("MINIO_BATCH_PREFIX", "hotel_booking_demand/incremental_batches").strip("/")

    spark = build_spark()
    try:
        create_table_if_needed(spark, table_name)
        for batch_file in batch_files:
            batch_id = batch_file.stem
            if batch_already_ingested(spark, table_name, batch_id) and not force:
                print(f"Skipping {batch_id}: already exists in {table_name}")
                continue

            object_key, storage_metadata = build_partitioned_batch_object_key(prefix, batch_file)
            df = read_and_enrich_batch(
                spark,
                local_batch_file=batch_file,
                object_key=object_key,
                bucket=bucket,
                storage_metadata=storage_metadata,
            )
            row_count = df.count()
            df.writeTo(table_name).append()
            print(f"Appended {row_count:,} rows from {batch_file.name} to {table_name}")
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-dir", type=Path, default=DEFAULT_BATCH_DIR, help="Directory containing batch_*.csv")
    parser.add_argument("--force", action="store_true", help="Append even when a batch_id already exists")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        ingest_batches(args.batch_dir, force=args.force)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
