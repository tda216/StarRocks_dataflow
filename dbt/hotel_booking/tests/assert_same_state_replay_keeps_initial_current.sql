SELECT
    booking_key,
    first_seen_batch_id
FROM {{ ref('int_current_hotel_bookings') }}
WHERE booking_key = 'hotel_booking_demand:1'
  AND first_seen_batch_id <> 'batch_001_initial'

UNION ALL

SELECT
    'hotel_booking_demand:1' AS booking_key,
    'missing_current_row' AS first_seen_batch_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('int_current_hotel_bookings') }}
    WHERE booking_key = 'hotel_booking_demand:1'
)
