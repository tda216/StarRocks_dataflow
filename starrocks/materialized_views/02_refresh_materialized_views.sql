-- Run after dbt has rebuilt fact_bookings.
REFRESH MATERIALIZED VIEW hotel_booking.mv_daily_booking_revenue WITH SYNC MODE;
REFRESH MATERIALIZED VIEW hotel_booking.mv_monthly_booking_revenue WITH SYNC MODE;
REFRESH MATERIALIZED VIEW hotel_booking.mv_hotel_performance WITH SYNC MODE;
