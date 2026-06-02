
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select guest_type
from `hotel_booking`.`mart_customer_type_performance`
where guest_type is null



  
  
      
    ) dbt_internal_test