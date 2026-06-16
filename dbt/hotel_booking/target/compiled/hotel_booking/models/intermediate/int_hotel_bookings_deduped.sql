

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_key, batch_id, record_hash
            ORDER BY row_ingestion_id, ingested_at
        ) AS duplicate_rank
    FROM `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
)
SELECT
    booking_key,
    batch_id,
    source_dataset,
    original_source_row_number,
    batch_sequence,
    batch_effective_at,
    batch_row_number,
    source_file_name,
    source_object_path,
    file_hash,
    record_hash,
    ingested_at,
    row_ingestion_id,
    synthetic_operation,
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
FROM ranked
WHERE duplicate_rank = 1