
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from
from `hotel_booking`.`scd_hotel_bookings`
where valid_from is null



  
  
      
    ) dbt_internal_test