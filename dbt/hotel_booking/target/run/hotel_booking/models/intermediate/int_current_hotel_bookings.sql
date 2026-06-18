
  create view `hotel_booking`.`int_current_hotel_bookings__dbt_tmp` as 

SELECT
    *
FROM `iceberg_catalog`.`hotel_booking_silver`.`current_hotel_bookings`;