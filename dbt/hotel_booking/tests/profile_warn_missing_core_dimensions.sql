{{ config(severity='warn') }}

SELECT
    booking_key,
    current_batch_id,
    hotel,
    country,
    market_segment,
    distribution_channel,
    reserved_room_type,
    assigned_room_type,
    customer_type
FROM {{ ref('int_booking_metrics') }}
WHERE hotel IS NULL
   OR country IS NULL
   OR country = 'Unknown'
   OR market_segment IS NULL
   OR market_segment = 'Unknown'
   OR distribution_channel IS NULL
   OR distribution_channel = 'Unknown'
   OR reserved_room_type IS NULL
   OR reserved_room_type = 'Unknown'
   OR assigned_room_type IS NULL
   OR assigned_room_type = 'Unknown'
   OR customer_type IS NULL
   OR customer_type = 'Unknown'
