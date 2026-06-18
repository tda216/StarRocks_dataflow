
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_current as value_field,
        count(*) as n_records

    from `hotel_booking`.`int_hotel_booking_versions`
    group by is_current

)

select *
from all_values
where value_field not in (
    0,1
)



  
  
      
    ) dbt_internal_test