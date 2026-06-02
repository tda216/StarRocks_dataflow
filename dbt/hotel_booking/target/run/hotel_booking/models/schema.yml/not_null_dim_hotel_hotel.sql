
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hotel
from `hotel_booking`.`dim_hotel`
where hotel is null



  
  
      
    ) dbt_internal_test