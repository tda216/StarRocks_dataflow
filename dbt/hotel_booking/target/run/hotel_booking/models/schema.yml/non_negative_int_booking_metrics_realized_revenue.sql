
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT *
FROM `hotel_booking`.`int_booking_metrics`
WHERE realized_revenue < 0


  
  
      
    ) dbt_internal_test