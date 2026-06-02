

SELECT DISTINCT
    reserved_room_type,
    assigned_room_type
FROM `hotel_booking`.`int_booking_metrics`
WHERE reserved_room_type IS NOT NULL
  AND assigned_room_type IS NOT NULL