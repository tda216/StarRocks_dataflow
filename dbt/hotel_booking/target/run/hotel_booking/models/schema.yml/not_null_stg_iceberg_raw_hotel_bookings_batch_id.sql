
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select batch_id
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where batch_id is null



  
  
      
    ) dbt_internal_test