
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from
from `hotel_booking`.`int_hotel_booking_versions`
where valid_from is null



  
  
      
    ) dbt_internal_test