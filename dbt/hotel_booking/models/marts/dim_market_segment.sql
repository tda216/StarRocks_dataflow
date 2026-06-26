{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    market_segment
FROM {{ ref('int_booking_metrics') }}
WHERE market_segment IS NOT NULL
