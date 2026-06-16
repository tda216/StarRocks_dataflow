SELECT
    booking_key,
    batch_id,
    COUNT(DISTINCT record_hash) AS distinct_record_hashes
FROM {{ ref('stg_iceberg_raw_hotel_bookings') }}
GROUP BY
    booking_key,
    batch_id
HAVING COUNT(DISTINCT record_hash) > 1
