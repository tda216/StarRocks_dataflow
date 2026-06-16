-- Validation checks for StarRocks Materialized Views.

SHOW MATERIALIZED VIEWS FROM hotel_booking;

SELECT 'mv_daily_booking_revenue' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mv_daily_booking_revenue
UNION ALL
SELECT 'mart_daily_booking_revenue' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mart_daily_booking_revenue
UNION ALL
SELECT 'mv_monthly_booking_revenue' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mv_monthly_booking_revenue
UNION ALL
SELECT 'mart_monthly_booking_revenue' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mart_monthly_booking_revenue
UNION ALL
SELECT 'mv_hotel_performance' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mv_hotel_performance
UNION ALL
SELECT 'mart_hotel_performance' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mart_hotel_performance;

SELECT
    'daily_total_bookings_diff' AS check_name,
    (
        SELECT SUM(total_bookings)
        FROM hotel_booking.mv_daily_booking_revenue
    ) - (
        SELECT SUM(total_bookings)
        FROM hotel_booking.mart_daily_booking_revenue
    ) AS diff_value
UNION ALL
SELECT
    'monthly_total_bookings_diff' AS check_name,
    (
        SELECT SUM(total_bookings)
        FROM hotel_booking.mv_monthly_booking_revenue
    ) - (
        SELECT SUM(total_bookings)
        FROM hotel_booking.mart_monthly_booking_revenue
    ) AS diff_value
UNION ALL
SELECT
    'hotel_bookings_diff' AS check_name,
    (
        SELECT SUM(bookings)
        FROM hotel_booking.mv_hotel_performance
    ) - (
        SELECT SUM(bookings)
        FROM hotel_booking.mart_hotel_performance
    ) AS diff_value;
