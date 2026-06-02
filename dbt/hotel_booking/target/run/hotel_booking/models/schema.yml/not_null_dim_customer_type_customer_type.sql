
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_type
from `hotel_booking`.`dim_customer_type`
where customer_type is null



  
  
      
    ) dbt_internal_test