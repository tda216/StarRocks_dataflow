
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hotel
from `hotel_booking`.`fact_bookings`
where hotel is null



  
  
      
    ) dbt_internal_test