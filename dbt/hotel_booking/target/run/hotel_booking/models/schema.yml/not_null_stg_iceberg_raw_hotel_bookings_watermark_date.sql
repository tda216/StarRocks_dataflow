
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select watermark_date
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where watermark_date is null



  
  
      
    ) dbt_internal_test