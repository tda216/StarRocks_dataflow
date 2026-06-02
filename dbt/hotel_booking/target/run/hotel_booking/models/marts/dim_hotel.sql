
  
    

  create table `hotel_booking`.`dim_hotel__dbt_tmp`
      DUPLICATE KEY (hotel)
    DISTRIBUTED BY HASH (hotel)BUCKETS 2
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    hotel
FROM `hotel_booking`.`int_booking_metrics`
WHERE hotel IS NOT NULL
  