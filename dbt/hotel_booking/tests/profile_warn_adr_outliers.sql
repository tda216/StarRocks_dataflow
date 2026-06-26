{{ config(severity='warn') }}

SELECT
    booking_key,
    current_batch_id,
    source_adr
FROM {{ ref('int_booking_metrics') }}
WHERE source_adr < 0
   OR source_adr > 500
