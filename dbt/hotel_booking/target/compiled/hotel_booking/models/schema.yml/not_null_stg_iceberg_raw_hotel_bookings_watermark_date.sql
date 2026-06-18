
    
    



select watermark_date
from `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
where watermark_date is null


