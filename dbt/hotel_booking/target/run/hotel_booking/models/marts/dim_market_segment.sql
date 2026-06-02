
  
    

  create table `hotel_booking`.`dim_market_segment__dbt_tmp`
      DUPLICATE KEY (market_segment)
    DISTRIBUTED BY HASH (market_segment)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    market_segment
FROM `hotel_booking`.`int_booking_metrics`
WHERE market_segment IS NOT NULL
  