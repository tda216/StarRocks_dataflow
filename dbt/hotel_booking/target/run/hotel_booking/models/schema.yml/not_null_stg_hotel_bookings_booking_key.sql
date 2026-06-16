
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select booking_key
from `hotel_booking`.`stg_hotel_bookings`
where booking_key is null



  
  
      
    ) dbt_internal_test