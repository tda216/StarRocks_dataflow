{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    country
FROM {{ ref('int_booking_metrics') }}
WHERE country IS NOT NULL
