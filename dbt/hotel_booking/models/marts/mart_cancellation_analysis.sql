{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['hotel', 'market_segment', 'distribution_channel'],
    distributed_by=['hotel'],
    buckets=8,
    properties={'replication_num': '1'}
) }}

SELECT
    hotel,
    market_segment,
    distribution_channel,
    deposit_type,
    lead_time_bucket,
    COUNT(*) AS total_bookings,
    SUM(is_cancelled) AS cancelled_bookings,
    SUM(is_cancelled) / NULLIF(COUNT(*), 0) AS cancellation_rate,
    AVG(lead_time) AS avg_lead_time
FROM {{ ref('fact_bookings') }}
GROUP BY
    hotel,
    market_segment,
    distribution_channel,
    deposit_type,
    lead_time_bucket
