
    
    



select batch_effective_at
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where batch_effective_at is null


