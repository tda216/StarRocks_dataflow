
  
    

  create table `hotel_booking`.`mart_cancellation_analysis__dbt_tmp`
      DUPLICATE KEY (hotel,market_segment,distribution_channel)
    DISTRIBUTED BY HASH (hotel)BUCKETS 8
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    hotel,
    market_segment,
    distribution_channel,
    deposit_type,
    lead_time_bucket,
    COUNT(*) AS total_bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    AVG(lead_time) AS avg_lead_time
FROM `hotel_booking`.`fact_bookings`
GROUP BY
    hotel,
    market_segment,
    distribution_channel,
    deposit_type,
    lead_time_bucket
  