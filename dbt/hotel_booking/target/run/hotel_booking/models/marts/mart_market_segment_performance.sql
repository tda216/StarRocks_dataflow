
  
    

  create table `hotel_booking`.`mart_market_segment_performance__dbt_tmp`
      DUPLICATE KEY (market_segment)
    DISTRIBUTED BY HASH (market_segment)BUCKETS 8
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    market_segment,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(lead_time) AS avg_lead_time
FROM `hotel_booking`.`fact_bookings`
GROUP BY market_segment
  