{{ config(
    materialized='table',
    engine='OLAP',
    table_type='DUPLICATE',
    keys=['date_day'],
    distributed_by=['date_day'],
    buckets=4,
    properties={'replication_num': '1'}
) }}

SELECT DISTINCT
    arrival_date AS date_day,
    arrival_date_year AS year_number,
    arrival_month_number AS month_number,
    arrival_date_month AS month_name,
    arrival_date_week_number AS week_number,
    arrival_date_day_of_month AS day_of_month
FROM {{ ref('int_booking_metrics') }}
WHERE arrival_date IS NOT NULL
