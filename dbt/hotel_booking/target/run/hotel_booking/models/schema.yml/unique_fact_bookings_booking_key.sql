
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    booking_key as unique_field,
    count(*) as n_records

from `hotel_booking`.`fact_bookings`
where booking_key is not null
group by booking_key
having count(*) > 1



  
  
      
    ) dbt_internal_test