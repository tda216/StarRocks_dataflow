{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['market_segment'],
    distributed_by=['market_segment'],
    buckets=8,
    properties={'replication_num': '1'}
) }}

SELECT
    market_segment,
    COUNT(*) AS bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr,
    AVG(lead_time) AS avg_lead_time
FROM {{ ref('fact_bookings') }}
GROUP BY market_segment
