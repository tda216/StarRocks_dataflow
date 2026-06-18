
    
    

select
    booking_key as unique_field,
    count(*) as n_records

from `hotel_booking`.`int_current_hotel_bookings`
where booking_key is not null
group by booking_key
having count(*) > 1


