
  
    

  create table `hotel_booking`.`mart_channel_performance__dbt_tmp`
      DUPLICATE KEY (distribution_channel)
    DISTRIBUTED BY HASH (distribution_channel)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    distribution_channel,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM `hotel_booking`.`fact_bookings`
GROUP BY distribution_channel
  