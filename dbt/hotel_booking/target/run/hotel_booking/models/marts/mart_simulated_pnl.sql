
  
    

  create table `hotel_booking`.`mart_simulated_pnl__dbt_tmp`
      DUPLICATE KEY (hotel)
    DISTRIBUTED BY HASH (hotel)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT
    hotel,
    COUNT(*) AS bookings,
    SUM(realized_revenue) AS realized_revenue,
    SUM(total_nights * 20) + SUM(realized_revenue * 0.15) AS simulated_cost,
    SUM(realized_revenue) - (SUM(total_nights * 20) + SUM(realized_revenue * 0.15)) AS simulated_margin
FROM `hotel_booking`.`fact_bookings`
GROUP BY hotel
  