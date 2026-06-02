
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select country
from `hotel_booking`.`mart_country_performance`
where country is null



  
  
      
    ) dbt_internal_test