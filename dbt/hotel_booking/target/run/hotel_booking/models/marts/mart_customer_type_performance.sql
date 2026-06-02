
  
    

  create table `hotel_booking`.`mart_customer_type_performance__dbt_tmp`
      DUPLICATE KEY (customer_type,guest_type)
    DISTRIBUTED BY HASH (customer_type)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    customer_type,
    guest_type,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(total_guests) AS avg_total_guests
FROM `hotel_booking`.`fact_bookings`
GROUP BY
    customer_type,
    guest_type
  