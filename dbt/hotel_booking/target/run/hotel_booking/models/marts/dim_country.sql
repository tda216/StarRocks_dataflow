
  
    

  create table `hotel_booking`.`dim_country__dbt_tmp`
      DUPLICATE KEY (country)
    DISTRIBUTED BY HASH (country)BUCKETS 8
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    country
FROM `hotel_booking`.`int_booking_metrics`
WHERE country IS NOT NULL
  