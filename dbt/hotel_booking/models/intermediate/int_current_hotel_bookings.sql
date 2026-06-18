{{ config(
    materialized='view'
) }}

SELECT
    *
FROM `{{ env_var('ICEBERG_CATALOG_NAME', 'iceberg_catalog') }}`.`{{ env_var('ICEBERG_SILVER_DATABASE', 'hotel_booking_silver') }}`.`{{ env_var('ICEBERG_SILVER_CURRENT_TABLE', 'current_hotel_bookings') }}`
