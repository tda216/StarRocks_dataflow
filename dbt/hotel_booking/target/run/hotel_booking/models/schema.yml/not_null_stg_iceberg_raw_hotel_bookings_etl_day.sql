
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select etl_day
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where etl_day is null



  
  
      
    ) dbt_internal_test