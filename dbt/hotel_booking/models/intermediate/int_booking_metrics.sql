{{ config(
    materialized='view'
) }}

WITH base AS (
    SELECT
        booking_key,
        booking_key AS booking_id,
        source_dataset,
        original_source_row_number,
        current_batch_id,
        current_batch_sequence,
        etl_year,
        etl_month,
        etl_day,
        etl_date,
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
        GREATEST(COALESCE(stays_in_weekend_nights, 0), 0)
            + GREATEST(COALESCE(stays_in_week_nights, 0), 0) AS total_nights,
        GREATEST(COALESCE(adults, 0), 0) AS adults,
        GREATEST(COALESCE(children, 0), 0) AS children,
        GREATEST(COALESCE(babies, 0), 0) AS babies,
        adr AS source_adr,
        CASE WHEN adr < 0 THEN 1 ELSE 0 END AS has_negative_adr,
        GREATEST(COALESCE(adr, 0), 0) AS adr,
        source_file_name AS source_file,
        ingested_at AS loaded_at
    FROM {{ ref('int_current_hotel_bookings') }}
),
metrics AS (
    SELECT
        *,
        adults + children + babies AS total_guests,
        adr * total_nights AS estimated_revenue,
        CASE
            WHEN is_cancelled = 0 THEN adr * total_nights
            ELSE 0
        END AS realized_revenue,
        CASE
            WHEN lead_time BETWEEN 0 AND 7 THEN '0-7 days'
            WHEN lead_time BETWEEN 8 AND 30 THEN '8-30 days'
            WHEN lead_time BETWEEN 31 AND 90 THEN '31-90 days'
            WHEN lead_time BETWEEN 91 AND 180 THEN '91-180 days'
            ELSE '180+ days'
        END AS lead_time_bucket,
        CASE
            WHEN total_nights = 0 THEN '0 nights'
            WHEN total_nights BETWEEN 1 AND 2 THEN '1-2 nights'
            WHEN total_nights BETWEEN 3 AND 5 THEN '3-5 nights'
            WHEN total_nights BETWEEN 6 AND 10 THEN '6-10 nights'
            ELSE '10+ nights'
        END AS stay_length_bucket,
        CASE
            WHEN adults = 1 AND children = 0 AND babies = 0 THEN 'solo'
            WHEN adults = 2 AND children = 0 AND babies = 0 THEN 'couple'
            WHEN adults + children + babies > 0 THEN 'family/group'
            ELSE 'unknown'
        END AS guest_type
    FROM base
)

SELECT
    booking_key,
    booking_id,
    source_dataset,
    original_source_row_number,
    current_batch_id,
    current_batch_sequence,
    etl_year,
    etl_month,
    etl_day,
    etl_date,
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
    is_repeated_guest,
    previous_cancellations,
    previous_bookings_not_canceled,
    booking_changes,
    days_in_waiting_list,
    required_car_parking_spaces,
    total_of_special_requests,
    is_cancelled,
    lead_time,
    total_nights,
    adults,
    children,
    babies,
    total_guests,
    source_adr,
    has_negative_adr,
    adr,
    estimated_revenue,
    realized_revenue,
    lead_time_bucket,
    stay_length_bucket,
    guest_type,
    source_file,
    loaded_at
FROM metrics
