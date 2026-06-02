

SELECT DISTINCT
    customer_type,
    is_repeated_guest
FROM `hotel_booking`.`int_booking_metrics`
WHERE customer_type IS NOT NULL