
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_start_date
from `hotel_booking`.`mart_monthly_booking_revenue`
where month_start_date is null



  
  
      
    ) dbt_internal_test