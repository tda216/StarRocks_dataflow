
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM `hotel_booking`.`mart_customer_type_performance`
    LIMIT 1
)


  
  
      
    ) dbt_internal_test