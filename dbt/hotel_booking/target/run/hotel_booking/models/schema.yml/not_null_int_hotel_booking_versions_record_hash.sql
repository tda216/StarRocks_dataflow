
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select record_hash
from `hotel_booking`.`int_hotel_booking_versions`
where record_hash is null



  
  
      
    ) dbt_internal_test