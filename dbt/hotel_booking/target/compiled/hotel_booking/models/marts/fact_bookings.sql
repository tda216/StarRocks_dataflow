

SELECT
    booking_id,
    arrival_date,
    hotel,
    country,
    market_segment,
    distribution_channel,
    reserved_room_type,
    assigned_room_type,
    customer_type,
    deposit_type,
    total_nights,
    total_guests,
    adr,
    estimated_revenue,
    realized_revenue,
    is_cancelled,
    lead_time,
    lead_time_bucket,
    stay_length_bucket,
    guest_type,
    reservation_status,
    reservation_status_date,
    has_negative_adr,
    source_file,
    loaded_at
FROM `hotel_booking`.`int_booking_metrics`