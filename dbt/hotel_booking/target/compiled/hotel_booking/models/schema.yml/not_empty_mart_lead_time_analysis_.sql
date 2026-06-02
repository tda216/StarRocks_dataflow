

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_lead_time_analysis`
    LIMIT 1
)

