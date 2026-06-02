{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['market_segment'],
    distributed_by=['market_segment'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    market_segment
FROM {{ ref('int_booking_metrics') }}
WHERE market_segment IS NOT NULL
