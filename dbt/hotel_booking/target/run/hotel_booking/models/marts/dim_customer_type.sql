
  
    

  create table `hotel_booking`.`dim_customer_type__dbt_tmp`
      DUPLICATE KEY (customer_type)
    DISTRIBUTED BY HASH (customer_type)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    customer_type,
    is_repeated_guest
FROM `hotel_booking`.`int_booking_metrics`
WHERE customer_type IS NOT NULL
  