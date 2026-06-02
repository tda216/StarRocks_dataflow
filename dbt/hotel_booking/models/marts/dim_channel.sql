{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['distribution_channel'],
    distributed_by=['distribution_channel'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    distribution_channel
FROM {{ ref('int_booking_metrics') }}
WHERE distribution_channel IS NOT NULL
