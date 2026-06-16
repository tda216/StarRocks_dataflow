{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['reserved_room_type', 'assigned_room_type'],
    distributed_by=['reserved_room_type'],
    buckets=8,
    properties={'replication_num': '1'}
) }}

SELECT
    reserved_room_type,
    assigned_room_type,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    SUM(total_nights) AS total_nights
FROM {{ ref('fact_bookings') }}
GROUP BY
    reserved_room_type,
    assigned_room_type
