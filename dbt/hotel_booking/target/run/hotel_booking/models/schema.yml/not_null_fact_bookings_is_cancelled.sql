
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_cancelled
from `hotel_booking`.`fact_bookings`
where is_cancelled is null



  
  
      
    ) dbt_internal_test