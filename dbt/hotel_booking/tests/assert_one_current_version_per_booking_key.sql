SELECT
    booking_key,
    COUNT(*) AS current_versions
FROM {{ ref('int_hotel_booking_versions') }}
WHERE is_current = 1
GROUP BY booking_key
HAVING COUNT(*) > 1
