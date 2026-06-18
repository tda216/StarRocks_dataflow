SELECT
    booking_key,
    COUNT(*) AS current_versions
FROM `hotel_booking`.`int_hotel_booking_versions`
WHERE is_current = 1
GROUP BY booking_key
HAVING COUNT(*) > 1