
  
    

  create table `hotel_booking`.`dim_date__dbt_tmp`
      DUPLICATE KEY (date_day)
    DISTRIBUTED BY HASH (date_day)BUCKETS 4
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

SELECT DISTINCT
    arrival_date AS date_day,
    arrival_date_year AS year_number,
    arrival_month_number AS month_number,
    arrival_date_month AS month_name,
    arrival_date_week_number AS week_number,
    arrival_date_day_of_month AS day_of_month
FROM `hotel_booking`.`int_booking_metrics`
WHERE arrival_date IS NOT NULL
  