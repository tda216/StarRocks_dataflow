{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['customer_type', 'guest_type'],
    distributed_by=['customer_type'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT
    customer_type,
    guest_type,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(total_guests) AS avg_total_guests
FROM {{ ref('fact_bookings') }}
GROUP BY
    customer_type,
    guest_type
