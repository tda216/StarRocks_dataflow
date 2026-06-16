{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['booking_key', 'valid_from'],
    distributed_by=['booking_key'],
    buckets=16,
    properties={'replication_num': '1'}
) }}

WITH ordered_records AS (
    SELECT
        *,
        LAG(record_hash) OVER (
            PARTITION BY booking_key
            ORDER BY batch_sequence, batch_effective_at, row_ingestion_id
        ) AS previous_record_hash
    FROM {{ ref('int_hotel_bookings_deduped') }}
),
change_records AS (
    SELECT *
    FROM ordered_records
    WHERE previous_record_hash IS NULL
       OR record_hash <> previous_record_hash
),
versioned AS (
    SELECT
        *,
        batch_effective_at AS valid_from,
        LEAD(batch_effective_at) OVER (
            PARTITION BY booking_key
            ORDER BY batch_sequence, batch_effective_at, row_ingestion_id
        ) AS valid_to
    FROM change_records
)
SELECT
    booking_key,
    valid_from,
    source_dataset,
    original_source_row_number,
    batch_id AS first_seen_batch_id,
    batch_sequence AS first_seen_batch_sequence,
    batch_effective_at,
    source_file_name,
    source_object_path,
    file_hash,
    record_hash,
    ingested_at,
    row_ingestion_id,
    synthetic_operation,
    valid_to,
    CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END AS is_current,
    hotel,
    is_canceled,
    lead_time,
    arrival_date_year,
    arrival_date_month,
    arrival_date_week_number,
    arrival_date_day_of_month,
    stays_in_weekend_nights,
    stays_in_week_nights,
    adults,
    children,
    babies,
    meal,
    country,
    market_segment,
    distribution_channel,
    is_repeated_guest,
    previous_cancellations,
    previous_bookings_not_canceled,
    reserved_room_type,
    assigned_room_type,
    booking_changes,
    deposit_type,
    agent,
    company,
    days_in_waiting_list,
    customer_type,
    adr,
    required_car_parking_spaces,
    total_of_special_requests,
    reservation_status,
    reservation_status_date
FROM versioned
