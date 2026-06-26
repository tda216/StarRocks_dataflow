{{ config(severity='warn') }}

SELECT
    booking_key,
    current_batch_id,
    arrival_date_year,
    arrival_month_number,
    arrival_date_day_of_month,
    arrival_date
FROM {{ ref('int_current_hotel_bookings') }}
WHERE arrival_date IS NULL
   OR arrival_date_year IS NULL
   OR arrival_month_number NOT BETWEEN 1 AND 12
   OR arrival_date_day_of_month NOT BETWEEN 1 AND 31
