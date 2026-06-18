
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select etl_month
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where etl_month is null



  
  
      
    ) dbt_internal_test