
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select assigned_room_type
from `hotel_booking`.`mart_room_performance`
where assigned_room_type is null



  
  
      
    ) dbt_internal_test