#!/usr/bin/env python3
"""Build Silver Iceberg tables from the Bronze raw hotel booking history."""

from __future__ import annotations

from ingest_batches_to_iceberg import build_spark, drop_corrupt_iceberg_table, ensure_table_columns, env, _is_corrupt_metadata_error


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

RAW_METADATA_COLUMNS = [
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
    "watermark_date",
    "raw_batch_sequence",
    "source_file_name",
    "source_object_path",
    "file_hash",
    "record_hash",
    "ingested_at",
    "row_ingestion_id",
    "synthetic_operation",
]

CURRENT_COLUMNS = [
    "booking_key",
    "source_dataset",
    "original_source_row_number",
    "first_seen_batch_id",
    "first_seen_batch_sequence",
    "batch_effective_at",
    "etl_year",
    "etl_month",
    "etl_day",
    "watermark_date",
    "raw_batch_sequence",
    "record_hash",
    "source_file_name",
    "source_object_path",
    "file_hash",
    "ingested_at",
    "row_ingestion_id",
    "synthetic_operation",
    "hotel",
    "is_canceled",
    "lead_time",
    "arrival_date_year",
    "arrival_date_month",
    "arrival_month_number",
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
    "arrival_date",
]

METRIC_COLUMNS = [
    "booking_key",
    "booking_id",
    "source_dataset",
    "original_source_row_number",
    "first_seen_batch_id",
    "first_seen_batch_sequence",
    "etl_year",
    "etl_month",
    "etl_day",
    "watermark_date",
    "raw_batch_sequence",
    "record_hash",
    "hotel",
    "arrival_date",
    "arrival_date_year",
    "arrival_month_number",
    "arrival_date_month",
    "arrival_date_week_number",
    "arrival_date_day_of_month",
    "country",
    "market_segment",
    "distribution_channel",
    "reserved_room_type",
    "assigned_room_type",
    "customer_type",
    "deposit_type",
    "reservation_status",
    "reservation_status_date",
    "meal",
    "agent",
    "company",
    "is_repeated_guest",
    "previous_cancellations",
    "previous_bookings_not_canceled",
    "booking_changes",
    "days_in_waiting_list",
    "required_car_parking_spaces",
    "total_of_special_requests",
    "is_cancelled",
    "lead_time",
    "total_nights",
    "adults",
    "children",
    "babies",
    "total_guests",
    "source_adr",
    "has_negative_adr",
    "adr",
    "estimated_revenue",
    "realized_revenue",
    "lead_time_bucket",
    "stay_length_bucket",
    "guest_type",
    "source_file",
    "loaded_at",
]


def sql_select_columns(columns: list[str]) -> str:
    return ",\n    ".join(columns)


def raw_history_projection() -> str:
    expressions: list[str] = []
    for column in [*RAW_METADATA_COLUMNS, *SOURCE_COLUMNS]:
        if column == "etl_year":
            expressions.append("COALESCE(etl_year, YEAR(batch_effective_at)) AS etl_year")
        elif column == "etl_month":
            expressions.append("COALESCE(etl_month, MONTH(batch_effective_at)) AS etl_month")
        elif column == "etl_day":
            expressions.append("COALESCE(etl_day, DAYOFMONTH(batch_effective_at)) AS etl_day")
        elif column == "watermark_date":
            expressions.append("COALESCE(watermark_date, DATE_FORMAT(batch_effective_at, 'yyyyMMdd')) AS watermark_date")
        elif column == "raw_batch_sequence":
            expressions.append("COALESCE(raw_batch_sequence, LPAD(CAST(batch_sequence AS STRING), 3, '0')) AS raw_batch_sequence")
        else:
            expressions.append(column)
    return ",\n    ".join(expressions)


def create_or_replace(spark, table_name: str, select_sql: str) -> None:
    print(f"\n==> Building {table_name}")
    create_sql = f"""
    CREATE OR REPLACE TABLE {table_name}
    USING iceberg
    AS
    {select_sql}
    """
    try:
        spark.sql(create_sql)
    except Exception as exc:
        if _is_corrupt_metadata_error(exc):
            print(
                f"Detected corrupt Iceberg metadata for {table_name}. "
                "Dropping the catalog entry and recreating the deterministic Silver table."
            )
            drop_corrupt_iceberg_table(spark, table_name)
            spark.sql(create_sql)
        else:
            raise
    row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}").collect()[0]["row_count"]
    print(f"{table_name}: {row_count:,} rows")


def build_silver_tables() -> None:
    catalog = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    bronze_database = env("ICEBERG_DATABASE", "hotel_booking_lakehouse")
    silver_database = env("ICEBERG_SILVER_DATABASE", "hotel_booking_silver")
    raw_table = env("ICEBERG_RAW_HISTORY_TABLE", "raw_hotel_bookings_history")

    raw_history = f"{catalog}.{bronze_database}.{raw_table}"
    deduped = f"{catalog}.{silver_database}.{env('ICEBERG_SILVER_DEDUPED_TABLE', 'deduped_hotel_bookings')}"
    current = f"{catalog}.{silver_database}.{env('ICEBERG_SILVER_CURRENT_TABLE', 'current_hotel_bookings')}"
    metrics = f"{catalog}.{silver_database}.{env('ICEBERG_SILVER_METRICS_TABLE', 'booking_metrics')}"
    legacy_versions = f"{catalog}.{silver_database}.hotel_booking_versions"

    spark = build_spark()
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{silver_database}")
        ensure_table_columns(spark, raw_history)
        spark.sql(f"DROP TABLE IF EXISTS {legacy_versions}")

        raw_columns = raw_history_projection()
        create_or_replace(
            spark,
            deduped,
            f"""
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY booking_key, batch_id, record_hash
                        ORDER BY row_ingestion_id, ingested_at
                    ) AS duplicate_rank
                FROM {raw_history}
            )
            SELECT
                {raw_columns}
            FROM ranked
            WHERE duplicate_rank = 1
            """,
        )

        create_or_replace(
            spark,
            current,
            f"""
            WITH ordered_records AS (
                SELECT
                    *,
                    LAG(record_hash) OVER (
                        PARTITION BY booking_key
                        ORDER BY batch_sequence, batch_effective_at, row_ingestion_id
                    ) AS previous_record_hash
                FROM {deduped}
            ),
            change_records AS (
                SELECT *
                FROM ordered_records
                WHERE previous_record_hash IS NULL
                   OR record_hash <> previous_record_hash
            ),
            current_records AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY booking_key
                        ORDER BY batch_sequence DESC, batch_effective_at DESC, row_ingestion_id DESC
                    ) AS current_rank
                FROM change_records
            ),
            cleaned AS (
                SELECT
                    booking_key,
                    source_dataset,
                    original_source_row_number,
                    batch_id AS first_seen_batch_id,
                    batch_sequence AS first_seen_batch_sequence,
                    batch_effective_at,
                    etl_year,
                    etl_month,
                    etl_day,
                    watermark_date,
                    raw_batch_sequence,
                    record_hash,
                    source_file_name,
                    source_object_path,
                    file_hash,
                    ingested_at,
                    row_ingestion_id,
                    synthetic_operation,
                    NULLIF(NULLIF(TRIM(hotel), ''), 'NULL') AS hotel,
                    NULLIF(NULLIF(TRIM(is_canceled), ''), 'NULL') AS is_canceled,
                    NULLIF(NULLIF(TRIM(lead_time), ''), 'NULL') AS lead_time,
                    NULLIF(NULLIF(TRIM(arrival_date_year), ''), 'NULL') AS arrival_date_year,
                    NULLIF(NULLIF(TRIM(arrival_date_month), ''), 'NULL') AS arrival_date_month,
                    NULLIF(NULLIF(TRIM(arrival_date_week_number), ''), 'NULL') AS arrival_date_week_number,
                    NULLIF(NULLIF(TRIM(arrival_date_day_of_month), ''), 'NULL') AS arrival_date_day_of_month,
                    NULLIF(NULLIF(TRIM(stays_in_weekend_nights), ''), 'NULL') AS stays_in_weekend_nights,
                    NULLIF(NULLIF(TRIM(stays_in_week_nights), ''), 'NULL') AS stays_in_week_nights,
                    NULLIF(NULLIF(TRIM(adults), ''), 'NULL') AS adults,
                    NULLIF(NULLIF(TRIM(children), ''), 'NULL') AS children,
                    NULLIF(NULLIF(TRIM(babies), ''), 'NULL') AS babies,
                    NULLIF(NULLIF(TRIM(meal), ''), 'NULL') AS meal,
                    NULLIF(NULLIF(TRIM(country), ''), 'NULL') AS country,
                    NULLIF(NULLIF(TRIM(market_segment), ''), 'NULL') AS market_segment,
                    NULLIF(NULLIF(TRIM(distribution_channel), ''), 'NULL') AS distribution_channel,
                    NULLIF(NULLIF(TRIM(is_repeated_guest), ''), 'NULL') AS is_repeated_guest,
                    NULLIF(NULLIF(TRIM(previous_cancellations), ''), 'NULL') AS previous_cancellations,
                    NULLIF(NULLIF(TRIM(previous_bookings_not_canceled), ''), 'NULL') AS previous_bookings_not_canceled,
                    NULLIF(NULLIF(TRIM(reserved_room_type), ''), 'NULL') AS reserved_room_type,
                    NULLIF(NULLIF(TRIM(assigned_room_type), ''), 'NULL') AS assigned_room_type,
                    NULLIF(NULLIF(TRIM(booking_changes), ''), 'NULL') AS booking_changes,
                    NULLIF(NULLIF(TRIM(deposit_type), ''), 'NULL') AS deposit_type,
                    NULLIF(NULLIF(TRIM(agent), ''), 'NULL') AS agent,
                    NULLIF(NULLIF(TRIM(company), ''), 'NULL') AS company,
                    NULLIF(NULLIF(TRIM(days_in_waiting_list), ''), 'NULL') AS days_in_waiting_list,
                    NULLIF(NULLIF(TRIM(customer_type), ''), 'NULL') AS customer_type,
                    NULLIF(NULLIF(TRIM(adr), ''), 'NULL') AS adr,
                    NULLIF(NULLIF(TRIM(required_car_parking_spaces), ''), 'NULL') AS required_car_parking_spaces,
                    NULLIF(NULLIF(TRIM(total_of_special_requests), ''), 'NULL') AS total_of_special_requests,
                    NULLIF(NULLIF(TRIM(reservation_status), ''), 'NULL') AS reservation_status,
                    NULLIF(NULLIF(TRIM(reservation_status_date), ''), 'NULL') AS reservation_status_date
                FROM current_records
                WHERE current_rank = 1
            ),
            typed AS (
                SELECT
                    booking_key,
                    source_dataset,
                    original_source_row_number,
                    first_seen_batch_id,
                    first_seen_batch_sequence,
                    batch_effective_at,
                    etl_year,
                    etl_month,
                    etl_day,
                    watermark_date,
                    raw_batch_sequence,
                    record_hash,
                    source_file_name,
                    source_object_path,
                    file_hash,
                    ingested_at,
                    row_ingestion_id,
                    synthetic_operation,
                    hotel,
                    CAST(is_canceled AS INT) AS is_canceled,
                    CAST(lead_time AS INT) AS lead_time,
                    CAST(arrival_date_year AS INT) AS arrival_date_year,
                    arrival_date_month,
                    CASE LOWER(arrival_date_month)
                        WHEN 'january' THEN 1
                        WHEN 'february' THEN 2
                        WHEN 'march' THEN 3
                        WHEN 'april' THEN 4
                        WHEN 'may' THEN 5
                        WHEN 'june' THEN 6
                        WHEN 'july' THEN 7
                        WHEN 'august' THEN 8
                        WHEN 'september' THEN 9
                        WHEN 'october' THEN 10
                        WHEN 'november' THEN 11
                        WHEN 'december' THEN 12
                    END AS arrival_month_number,
                    CAST(arrival_date_week_number AS INT) AS arrival_date_week_number,
                    CAST(arrival_date_day_of_month AS INT) AS arrival_date_day_of_month,
                    CAST(stays_in_weekend_nights AS INT) AS stays_in_weekend_nights,
                    CAST(stays_in_week_nights AS INT) AS stays_in_week_nights,
                    CAST(adults AS INT) AS adults,
                    CAST(children AS INT) AS children,
                    CAST(babies AS INT) AS babies,
                    meal,
                    COALESCE(country, 'Unknown') AS country,
                    COALESCE(market_segment, 'Unknown') AS market_segment,
                    COALESCE(distribution_channel, 'Unknown') AS distribution_channel,
                    CAST(is_repeated_guest AS INT) AS is_repeated_guest,
                    CAST(previous_cancellations AS INT) AS previous_cancellations,
                    CAST(previous_bookings_not_canceled AS INT) AS previous_bookings_not_canceled,
                    COALESCE(reserved_room_type, 'Unknown') AS reserved_room_type,
                    COALESCE(assigned_room_type, 'Unknown') AS assigned_room_type,
                    CAST(booking_changes AS INT) AS booking_changes,
                    COALESCE(deposit_type, 'Unknown') AS deposit_type,
                    agent,
                    company,
                    CAST(days_in_waiting_list AS INT) AS days_in_waiting_list,
                    COALESCE(customer_type, 'Unknown') AS customer_type,
                    CAST(adr AS DECIMAL(18, 2)) AS adr,
                    CAST(required_car_parking_spaces AS INT) AS required_car_parking_spaces,
                    CAST(total_of_special_requests AS INT) AS total_of_special_requests,
                    COALESCE(reservation_status, 'Unknown') AS reservation_status,
                    CAST(reservation_status_date AS DATE) AS reservation_status_date
                FROM cleaned
            )
            SELECT
                *,
                CAST(
                    CONCAT(
                        CAST(arrival_date_year AS STRING),
                        '-',
                        LPAD(CAST(arrival_month_number AS STRING), 2, '0'),
                        '-',
                        LPAD(CAST(arrival_date_day_of_month AS STRING), 2, '0')
                    ) AS DATE
                ) AS arrival_date
            FROM typed
            """,
        )

        metric_columns = sql_select_columns(METRIC_COLUMNS)
        create_or_replace(
            spark,
            metrics,
            f"""
            WITH bookings AS (
                SELECT * FROM {current}
            )
            SELECT
                booking_key,
                booking_key AS booking_id,
                source_dataset,
                original_source_row_number,
                first_seen_batch_id,
                first_seen_batch_sequence,
                etl_year,
                etl_month,
                etl_day,
                watermark_date,
                raw_batch_sequence,
                record_hash,
                hotel,
                arrival_date,
                arrival_date_year,
                arrival_month_number,
                arrival_date_month,
                arrival_date_week_number,
                arrival_date_day_of_month,
                country,
                market_segment,
                distribution_channel,
                reserved_room_type,
                assigned_room_type,
                customer_type,
                deposit_type,
                reservation_status,
                reservation_status_date,
                meal,
                agent,
                company,
                COALESCE(is_repeated_guest, 0) AS is_repeated_guest,
                COALESCE(previous_cancellations, 0) AS previous_cancellations,
                COALESCE(previous_bookings_not_canceled, 0) AS previous_bookings_not_canceled,
                COALESCE(booking_changes, 0) AS booking_changes,
                COALESCE(days_in_waiting_list, 0) AS days_in_waiting_list,
                COALESCE(required_car_parking_spaces, 0) AS required_car_parking_spaces,
                COALESCE(total_of_special_requests, 0) AS total_of_special_requests,
                COALESCE(is_canceled, 0) AS is_cancelled,
                COALESCE(lead_time, 0) AS lead_time,
                GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) AS total_nights,
                COALESCE(adults, 0) AS adults,
                COALESCE(children, 0) AS children,
                COALESCE(babies, 0) AS babies,
                COALESCE(adults, 0) + COALESCE(children, 0) + COALESCE(babies, 0) AS total_guests,
                adr AS source_adr,
                CASE WHEN COALESCE(adr, 0) < 0 THEN 1 ELSE 0 END AS has_negative_adr,
                GREATEST(COALESCE(adr, 0), 0) AS adr,
                GREATEST(COALESCE(adr, 0), 0)
                    * GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) AS estimated_revenue,
                CASE
                    WHEN COALESCE(is_canceled, 0) = 0 THEN
                        GREATEST(COALESCE(adr, 0), 0)
                        * GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0)
                    ELSE 0
                END AS realized_revenue,
                CASE
                    WHEN COALESCE(lead_time, 0) BETWEEN 0 AND 7 THEN '0-7 days'
                    WHEN COALESCE(lead_time, 0) BETWEEN 8 AND 30 THEN '8-30 days'
                    WHEN COALESCE(lead_time, 0) BETWEEN 31 AND 90 THEN '31-90 days'
                    WHEN COALESCE(lead_time, 0) BETWEEN 91 AND 180 THEN '91-180 days'
                    ELSE '180+ days'
                END AS lead_time_bucket,
                CASE
                    WHEN GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) = 0 THEN '0 night'
                    WHEN GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) BETWEEN 1 AND 2 THEN '1-2 nights'
                    WHEN GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) BETWEEN 3 AND 5 THEN '3-5 nights'
                    WHEN GREATEST(COALESCE(stays_in_weekend_nights, 0) + COALESCE(stays_in_week_nights, 0), 0) BETWEEN 6 AND 10 THEN '6-10 nights'
                    ELSE '10+ nights'
                END AS stay_length_bucket,
                CASE
                    WHEN COALESCE(adults, 0) + COALESCE(children, 0) + COALESCE(babies, 0) <= 0 THEN 'unknown'
                    WHEN COALESCE(adults, 0) = 1 AND COALESCE(children, 0) + COALESCE(babies, 0) = 0 THEN 'solo'
                    WHEN COALESCE(adults, 0) = 2 AND COALESCE(children, 0) + COALESCE(babies, 0) = 0 THEN 'couple'
                    ELSE 'family/group'
                END AS guest_type,
                source_object_path AS source_file,
                ingested_at AS loaded_at
            FROM bookings
            """,
        )

        # Force Spark analysis of the final projection names before StarRocks/dbt reads them.
        spark.sql(f"SELECT {metric_columns} FROM {metrics} LIMIT 1").collect()
    finally:
        spark.stop()


def main() -> int:
    try:
        build_silver_tables()
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
