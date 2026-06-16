

WITH bookings AS (
    SELECT * FROM `hotel_booking`.`stg_hotel_bookings`
)
SELECT
    booking_key,
    booking_key AS booking_id,
    source_dataset,
    original_source_row_number,
    first_seen_batch_id,
    first_seen_batch_sequence,
    valid_from,
    valid_to,
    is_current,
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