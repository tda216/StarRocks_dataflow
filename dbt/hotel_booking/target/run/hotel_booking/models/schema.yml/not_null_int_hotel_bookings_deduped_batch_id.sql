
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select batch_id
from `hotel_booking`.`int_hotel_bookings_deduped`
where batch_id is null



  
  
      
    ) dbt_internal_test