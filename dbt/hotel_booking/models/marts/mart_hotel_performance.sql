{{ config(
    materialized='view'
) }}

SELECT
    hotel,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(lead_time) AS avg_lead_time,
    SUM(total_nights) AS total_nights
FROM {{ ref('fact_bookings') }}
GROUP BY hotel
