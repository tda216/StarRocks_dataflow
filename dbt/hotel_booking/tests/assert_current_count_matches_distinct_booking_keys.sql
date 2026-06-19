WITH expected AS (
    SELECT COUNT(DISTINCT booking_key) AS row_count
    FROM {{ ref('stg_iceberg_raw_hotel_bookings') }}
),
actual AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('int_current_hotel_bookings') }}
)

SELECT
    actual.row_count AS actual_current_rows,
    expected.row_count AS expected_distinct_booking_keys
FROM actual
CROSS JOIN expected
WHERE actual.row_count <> expected.row_count
