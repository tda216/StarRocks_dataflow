{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['country'],
    distributed_by=['country'],
    buckets=8,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    country
FROM {{ ref('int_booking_metrics') }}
WHERE country IS NOT NULL
