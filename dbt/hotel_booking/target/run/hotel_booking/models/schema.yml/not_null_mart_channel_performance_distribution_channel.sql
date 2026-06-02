
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select distribution_channel
from `hotel_booking`.`mart_channel_performance`
where distribution_channel is null



  
  
      
    ) dbt_internal_test