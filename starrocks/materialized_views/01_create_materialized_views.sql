-- StarRocks Materialized Views for the pipeline optimization step.
-- These MVs do not replace dbt mart tables. They demonstrate precomputed
-- aggregation and query rewrite from hotel_booking.fact_bookings.

DROP MATERIALIZED VIEW IF EXISTS hotel_booking.mv_daily_booking_revenue;
CREATE MATERIALIZED VIEW hotel_booking.mv_daily_booking_revenue
DISTRIBUTED BY HASH(arrival_date) BUCKETS 8
REFRESH MANUAL
AS
SELECT
    arrival_date,
    COUNT(*) AS total_bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    COUNT(*) - SUM(is_cancelled) AS successful_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(total_nights) AS total_nights,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS average_adr
FROM hotel_booking.fact_bookings
WHERE arrival_date IS NOT NULL
GROUP BY arrival_date;

DROP MATERIALIZED VIEW IF EXISTS hotel_booking.mv_monthly_booking_revenue;
CREATE MATERIALIZED VIEW hotel_booking.mv_monthly_booking_revenue
DISTRIBUTED BY HASH(year_number, month_number) BUCKETS 4
REFRESH MANUAL
AS
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
FROM hotel_booking.fact_bookings
WHERE arrival_date IS NOT NULL
GROUP BY
    YEAR(arrival_date),
    MONTH(arrival_date),
    DATE_TRUNC('month', arrival_date);

DROP MATERIALIZED VIEW IF EXISTS hotel_booking.mv_hotel_performance;
CREATE MATERIALIZED VIEW hotel_booking.mv_hotel_performance
DISTRIBUTED BY HASH(hotel) BUCKETS 2
REFRESH MANUAL
AS
SELECT
    hotel,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(lead_time) AS avg_lead_time,
    SUM(total_nights) AS total_nights
FROM hotel_booking.fact_bookings
GROUP BY hotel;
