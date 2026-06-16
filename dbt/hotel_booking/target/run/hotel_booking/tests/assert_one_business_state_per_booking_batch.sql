
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    booking_key,
    batch_id,
    COUNT(DISTINCT record_hash) AS distinct_record_hashes
FROM `hotel_booking`.`stg_iceberg_raw_hotel_bookings`
GROUP BY
    booking_key,
    batch_id
HAVING COUNT(DISTINCT record_hash) > 1
  
  
      
    ) dbt_internal_test