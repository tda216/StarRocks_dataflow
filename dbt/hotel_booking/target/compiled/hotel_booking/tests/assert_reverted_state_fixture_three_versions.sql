WITH fixture AS (
    SELECT COUNT(*) AS version_count
    FROM `hotel_booking`.`scd_hotel_bookings`
    WHERE booking_key = 'hotel_booking_demand:2'
)
SELECT version_count
FROM fixture
WHERE version_count <> 3