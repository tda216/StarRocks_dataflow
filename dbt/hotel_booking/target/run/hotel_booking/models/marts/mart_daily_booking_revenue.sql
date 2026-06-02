
  
    

  create table `hotel_booking`.`mart_daily_booking_revenue__dbt_tmp`
      DUPLICATE KEY (arrival_date)
    DISTRIBUTED BY HASH (arrival_date)BUCKETS 8
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

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
FROM `hotel_booking`.`fact_bookings`
WHERE arrival_date IS NOT NULL
GROUP BY arrival_date
  