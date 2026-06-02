
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lead_time_bucket
from `hotel_booking`.`mart_cancellation_analysis`
where lead_time_bucket is null



  
  
      
    ) dbt_internal_test