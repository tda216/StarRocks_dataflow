SELECT
    booking_key,
    current_batch_id
FROM {{ ref('int_current_hotel_bookings') }}
WHERE booking_key = 'hotel_booking_demand:2'
  AND current_batch_id <> 'batch_005_reverted_state'

UNION ALL

SELECT
    'hotel_booking_demand:2' AS booking_key,
    'missing_current_row' AS current_batch_id
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('int_current_hotel_bookings') }}
    WHERE booking_key = 'hotel_booking_demand:2'
)
