

SELECT
    reserved_room_type,
    assigned_room_type,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    SUM(total_nights) AS total_nights
FROM `hotel_booking`.`fact_bookings`
GROUP BY
    reserved_room_type,
    assigned_room_type