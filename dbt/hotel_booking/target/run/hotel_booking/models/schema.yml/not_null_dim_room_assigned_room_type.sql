
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select assigned_room_type
from `hotel_booking`.`dim_room`
where assigned_room_type is null



  
  
      
    ) dbt_internal_test