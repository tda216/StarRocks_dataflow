
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    booking_key,
    COUNT(*) AS current_versions
FROM `hotel_booking`.`scd_hotel_bookings`
WHERE is_current = 1
GROUP BY booking_key
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test