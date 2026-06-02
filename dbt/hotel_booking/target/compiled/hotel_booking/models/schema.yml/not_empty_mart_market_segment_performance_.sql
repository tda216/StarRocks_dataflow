

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_market_segment_performance`
    LIMIT 1
)

