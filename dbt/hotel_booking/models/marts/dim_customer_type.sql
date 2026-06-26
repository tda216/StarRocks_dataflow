{{ config(
    materialized='view'
) }}

SELECT DISTINCT
    customer_type,
    is_repeated_guest
FROM {{ ref('int_booking_metrics') }}
WHERE customer_type IS NOT NULL
