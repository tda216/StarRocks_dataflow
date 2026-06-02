
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select arrival_date
from `hotel_booking`.`fact_bookings`
where arrival_date is null



  
  
      
    ) dbt_internal_test