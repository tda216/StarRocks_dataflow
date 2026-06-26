{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    hotel
FROM {{ ref('int_booking_metrics') }}
WHERE hotel IS NOT NULL
