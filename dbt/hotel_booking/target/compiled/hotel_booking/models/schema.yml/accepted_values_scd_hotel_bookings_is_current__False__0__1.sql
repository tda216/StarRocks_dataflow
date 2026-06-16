
    
    

with all_values as (

    select
        is_current as value_field,
        count(*) as n_records

    from `hotel_booking`.`scd_hotel_bookings`
    group by is_current

)

select *
from all_values
where value_field not in (
    0,1
)


