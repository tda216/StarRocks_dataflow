
  
    

  create table `hotel_booking`.`mart_room_performance__dbt_tmp`
      DUPLICATE KEY (reserved_room_type,assigned_room_type)
    DISTRIBUTED BY HASH (reserved_room_type)BUCKETS 8
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    reserved_room_type,
    assigned_room_type,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    SUM(total_nights) AS total_nights
FROM `hotel_booking`.`fact_bookings`
GROUP BY
    reserved_room_type,
    assigned_room_type
  