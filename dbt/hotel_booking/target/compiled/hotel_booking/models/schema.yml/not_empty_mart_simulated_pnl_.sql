

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_simulated_pnl`
    LIMIT 1
)

