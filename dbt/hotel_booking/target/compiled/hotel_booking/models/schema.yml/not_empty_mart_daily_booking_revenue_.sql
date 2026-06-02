

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_daily_booking_revenue`
    LIMIT 1
)

