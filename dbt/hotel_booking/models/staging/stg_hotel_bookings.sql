{{ config(
    materialized='table',
    engine='OLAP',
    table_type='PRIMARY',
    keys=['booking_key'],
    distributed_by=['booking_key'],
    buckets=16,
    properties={'replication_num': '1'}
) }}

WITH current_scd AS (
    SELECT *
    FROM {{ ref('scd_hotel_bookings') }}
    WHERE is_current = 1
),
cleaned AS (
    SELECT
        booking_key,
        source_dataset,
        original_source_row_number,
        first_seen_batch_id,
        first_seen_batch_sequence,
        batch_effective_at,
        valid_from,
        valid_to,
        is_current,
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
    FROM current_scd
),
typed AS (
    SELECT
        booking_key,
        source_dataset,
        original_source_row_number,
        first_seen_batch_id,
        first_seen_batch_sequence,
        batch_effective_at,
        valid_from,
        valid_to,
        is_current,
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
            CAST(arrival_date_year AS VARCHAR),
            '-',
            LPAD(CAST(arrival_month_number AS VARCHAR), 2, '0'),
            '-',
            LPAD(CAST(arrival_date_day_of_month AS VARCHAR), 2, '0')
        ) AS DATE
    ) AS arrival_date
FROM typed
