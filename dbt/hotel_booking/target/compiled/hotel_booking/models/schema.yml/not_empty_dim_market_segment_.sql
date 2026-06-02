

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`dim_market_segment`
    LIMIT 1
)

