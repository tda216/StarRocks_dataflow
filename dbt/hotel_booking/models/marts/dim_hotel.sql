{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['hotel'],
    distributed_by=['hotel'],
    buckets=2,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    hotel
FROM {{ ref('int_booking_metrics') }}
WHERE hotel IS NOT NULL
