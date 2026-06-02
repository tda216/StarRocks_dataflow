{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['hotel'],
    distributed_by=['hotel'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT
    hotel,
    COUNT(*) AS bookings,
    SUM(realized_revenue) AS realized_revenue,
    SUM(total_nights * 20) + SUM(realized_revenue * 0.15) AS simulated_cost,
    SUM(realized_revenue) - (SUM(total_nights * 20) + SUM(realized_revenue * 0.15)) AS simulated_margin
FROM {{ ref('fact_bookings') }}
GROUP BY hotel
