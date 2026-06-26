{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    distribution_channel
FROM {{ ref('int_booking_metrics') }}
WHERE distribution_channel IS NOT NULL
