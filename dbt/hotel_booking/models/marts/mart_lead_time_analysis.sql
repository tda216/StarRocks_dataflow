{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['lead_time_bucket'],
    distributed_by=['lead_time_bucket'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT
    lead_time_bucket,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM {{ ref('fact_bookings') }}
GROUP BY lead_time_bucket
