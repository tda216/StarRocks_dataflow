
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  WITH fixture AS (
    SELECT COUNT(*) AS version_count
    FROM `hotel_booking`.`int_hotel_booking_versions`
    WHERE booking_key = 'hotel_booking_demand:2'
)
SELECT version_count
FROM fixture
WHERE version_count <> 3
  
  
      
    ) dbt_internal_test