WITH fixture AS (
    SELECT COUNT(*) AS version_count
    FROM {{ ref('int_hotel_booking_versions') }}
    WHERE booking_key = 'hotel_booking_demand:1'
)
SELECT version_count
FROM fixture
WHERE version_count <> 1
