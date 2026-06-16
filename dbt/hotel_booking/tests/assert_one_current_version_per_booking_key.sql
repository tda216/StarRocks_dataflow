SELECT
    booking_key,
    COUNT(*) AS current_versions
FROM {{ ref('scd_hotel_bookings') }}
WHERE is_current = 1
GROUP BY booking_key
HAVING COUNT(*) > 1
