{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['customer_type'],
    distributed_by=['customer_type'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    customer_type,
    is_repeated_guest
FROM {{ ref('int_booking_metrics') }}
WHERE customer_type IS NOT NULL
