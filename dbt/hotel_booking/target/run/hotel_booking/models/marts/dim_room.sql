
  
    

  create table `hotel_booking`.`dim_room__dbt_tmp`
      DUPLICATE KEY (reserved_room_type,assigned_room_type)
    DISTRIBUTED BY HASH (reserved_room_type)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    reserved_room_type,
    assigned_room_type
FROM `hotel_booking`.`int_booking_metrics`
WHERE reserved_room_type IS NOT NULL
  AND assigned_room_type IS NOT NULL
  