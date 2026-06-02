
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select arrival_date_year
from `hotel_booking`.`raw_hotel_bookings`
where arrival_date_year is null



  
  
      
    ) dbt_internal_test