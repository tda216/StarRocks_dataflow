
  
    

  create table `hotel_booking`.`mart_hotel_performance__dbt_tmp`
      DUPLICATE KEY (hotel)
    DISTRIBUTED BY HASH (hotel)BUCKETS 2
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    hotel,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(lead_time) AS avg_lead_time,
    SUM(total_nights) AS total_nights
FROM `hotel_booking`.`fact_bookings`
GROUP BY hotel
  