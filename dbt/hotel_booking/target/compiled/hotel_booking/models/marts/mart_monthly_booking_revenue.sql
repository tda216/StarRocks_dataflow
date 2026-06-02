

SELECT
    YEAR(arrival_date) AS year_number,
    MONTH(arrival_date) AS month_number,
    DATE_TRUNC('month', arrival_date) AS month_start_date,
    COUNT(*) AS total_bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    COUNT(*) - SUM(is_cancelled) AS successful_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(total_nights) AS total_nights,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS average_adr
FROM `hotel_booking`.`fact_bookings`
WHERE arrival_date IS NOT NULL
GROUP BY
    YEAR(arrival_date),
    MONTH(arrival_date),
    DATE_TRUNC('month', arrival_date)