
  
    

  create table `hotel_booking`.`mart_lead_time_analysis__dbt_tmp`
      DUPLICATE KEY (lead_time_bucket)
    DISTRIBUTED BY HASH (lead_time_bucket)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    lead_time_bucket,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM `hotel_booking`.`fact_bookings`
GROUP BY lead_time_bucket
  