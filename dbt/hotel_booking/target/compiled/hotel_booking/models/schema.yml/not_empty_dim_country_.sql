

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`dim_country`
    LIMIT 1
)

