
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market_segment
from `hotel_booking`.`mart_market_segment_performance`
where market_segment is null



  
  
      
    ) dbt_internal_test