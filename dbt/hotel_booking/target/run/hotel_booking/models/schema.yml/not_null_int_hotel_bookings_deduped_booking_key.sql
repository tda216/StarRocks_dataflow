
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select booking_key
from `hotel_booking`.`int_hotel_bookings_deduped`
where booking_key is null



  
  
      
    ) dbt_internal_test