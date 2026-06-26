{{ config(severity='warn') }}

SELECT
    booking_key,
    current_batch_id,
    total_nights
FROM {{ ref('int_booking_metrics') }}
WHERE total_nights = 0
