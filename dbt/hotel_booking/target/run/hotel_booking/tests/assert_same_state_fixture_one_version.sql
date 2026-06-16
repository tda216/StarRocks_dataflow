
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  WITH fixture AS (
    SELECT COUNT(*) AS version_count
    FROM `hotel_booking`.`scd_hotel_bookings`
    WHERE booking_key = 'hotel_booking_demand:1'
)
SELECT version_count
FROM fixture
WHERE version_count <> 1
  
  
      
    ) dbt_internal_test