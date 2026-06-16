
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    a.booking_key,
    a.valid_from AS a_valid_from,
    a.valid_to AS a_valid_to,
    b.valid_from AS b_valid_from,
    b.valid_to AS b_valid_to
FROM `hotel_booking`.`scd_hotel_bookings` a
JOIN `hotel_booking`.`scd_hotel_bookings` b
  ON a.booking_key = b.booking_key
 AND a.valid_from < COALESCE(b.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND b.valid_from < COALESCE(a.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND a.valid_from <> b.valid_from
  
  
      
    ) dbt_internal_test