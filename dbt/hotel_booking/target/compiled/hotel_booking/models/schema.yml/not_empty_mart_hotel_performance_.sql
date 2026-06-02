

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_hotel_performance`
    LIMIT 1
)

