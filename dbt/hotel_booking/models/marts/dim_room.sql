{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['reserved_room_type', 'assigned_room_type'],
    distributed_by=['reserved_room_type'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    reserved_room_type,
    assigned_room_type
FROM {{ ref('int_booking_metrics') }}
WHERE reserved_room_type IS NOT NULL
  AND assigned_room_type IS NOT NULL
