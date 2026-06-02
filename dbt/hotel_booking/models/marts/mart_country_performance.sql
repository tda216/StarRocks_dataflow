{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['country'],
    distributed_by=['country'],
    buckets=16,
    properties={'replication_num': '1'}
) }}

SELECT
    country,
    COUNT(*) AS bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(estimated_revenue) AS estimated_revenue,
    SUM(realized_revenue) AS realized_revenue,
    AVG(adr) AS avg_adr
FROM {{ ref('fact_bookings') }}
GROUP BY country
