
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT *
FROM `hotel_booking`.`fact_bookings`
WHERE total_nights < 0


  
  
      
    ) dbt_internal_test