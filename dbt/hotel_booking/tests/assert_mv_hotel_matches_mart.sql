SELECT
    'bookings' AS metric_name,
    (
        SELECT COALESCE(SUM(bookings), 0)
        FROM {{ ref('mv_hotel_performance') }}
    ) - (
        SELECT COALESCE(SUM(bookings), 0)
        FROM {{ ref('mart_hotel_performance') }}
    ) AS diff_value
WHERE (
    SELECT COALESCE(SUM(bookings), 0)
    FROM {{ ref('mv_hotel_performance') }}
) <> (
    SELECT COALESCE(SUM(bookings), 0)
    FROM {{ ref('mart_hotel_performance') }}
)

UNION ALL

SELECT
    'realized_revenue' AS metric_name,
    (
        SELECT COALESCE(SUM(realized_revenue), 0)
        FROM {{ ref('mv_hotel_performance') }}
    ) - (
        SELECT COALESCE(SUM(realized_revenue), 0)
        FROM {{ ref('mart_hotel_performance') }}
    ) AS diff_value
WHERE (
    SELECT COALESCE(SUM(realized_revenue), 0)
    FROM {{ ref('mv_hotel_performance') }}
) <> (
    SELECT COALESCE(SUM(realized_revenue), 0)
    FROM {{ ref('mart_hotel_performance') }}
)
