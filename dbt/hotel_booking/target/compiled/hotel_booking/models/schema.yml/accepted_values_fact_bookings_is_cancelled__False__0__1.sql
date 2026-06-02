
    
    

with all_values as (

    select
        is_cancelled as value_field,
        count(*) as n_records

    from `hotel_booking`.`fact_bookings`
    group by is_cancelled

)

select *
from all_values
where value_field not in (
    0,1
)


