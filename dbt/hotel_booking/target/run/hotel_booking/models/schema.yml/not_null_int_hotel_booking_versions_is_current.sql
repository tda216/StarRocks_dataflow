
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_current
from `hotel_booking`.`int_hotel_booking_versions`
where is_current is null



  
  
      
    ) dbt_internal_test