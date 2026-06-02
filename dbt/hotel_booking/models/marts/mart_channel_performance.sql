{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['distribution_channel'],
    distributed_by=['distribution_channel'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT
    distribution_channel,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM {{ ref('fact_bookings') }}
GROUP BY distribution_channel
