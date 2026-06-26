{{ config(severity='warn') }}

SELECT
    booking_key,
    current_batch_id,
    total_guests
FROM {{ ref('int_booking_metrics') }}
WHERE total_guests = 0
