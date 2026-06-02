

SELECT
    country,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM `hotel_booking`.`fact_bookings`
GROUP BY country