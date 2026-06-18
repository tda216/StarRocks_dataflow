
  create view `hotel_booking`.`int_booking_metrics__dbt_tmp` as 

SELECT
    *
FROM `iceberg_catalog`.`hotel_booking_silver`.`booking_metrics`;