SELECT
    'total_bookings' AS metric_name,
    (
        SELECT COALESCE(SUM(total_bookings), 0)
        FROM {{ ref('mv_daily_booking_revenue') }}
    ) - (
        SELECT COALESCE(SUM(total_bookings), 0)
        FROM {{ ref('mart_daily_booking_revenue') }}
    ) AS diff_value
WHERE (
    SELECT COALESCE(SUM(total_bookings), 0)
    FROM {{ ref('mv_daily_booking_revenue') }}
) <> (
    SELECT COALESCE(SUM(total_bookings), 0)
    FROM {{ ref('mart_daily_booking_revenue') }}
)

UNION ALL

SELECT
    'realized_revenue' AS metric_name,
    (
        SELECT COALESCE(SUM(realized_revenue), 0)
        FROM {{ ref('mv_daily_booking_revenue') }}
    ) - (
        SELECT COALESCE(SUM(realized_revenue), 0)
        FROM {{ ref('mart_daily_booking_revenue') }}
    ) AS diff_value
WHERE (
    SELECT COALESCE(SUM(realized_revenue), 0)
    FROM {{ ref('mv_daily_booking_revenue') }}
) <> (
    SELECT COALESCE(SUM(realized_revenue), 0)
    FROM {{ ref('mart_daily_booking_revenue') }}
)
