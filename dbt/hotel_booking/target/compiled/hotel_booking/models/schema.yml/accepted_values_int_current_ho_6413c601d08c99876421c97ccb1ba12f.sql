
    
    

with all_values as (

    select
        is_canceled as value_field,
        count(*) as n_records

    from `hotel_booking`.`int_current_hotel_bookings`
    group by is_canceled

)

select *
from all_values
where value_field not in (
    0,1
)


