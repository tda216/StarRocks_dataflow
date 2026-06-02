
  
    

  create table `hotel_booking`.`dim_channel__dbt_tmp`
      DUPLICATE KEY (distribution_channel)
    DISTRIBUTED BY HASH (distribution_channel)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    distribution_channel
FROM `hotel_booking`.`int_booking_metrics`
WHERE distribution_channel IS NOT NULL
  