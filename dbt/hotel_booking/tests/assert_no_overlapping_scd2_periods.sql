WITH ordered_versions AS (
    SELECT
        booking_key,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY booking_key
            ORDER BY valid_from
        ) AS next_valid_from
    FROM {{ ref('int_hotel_booking_versions') }}
)

SELECT
    booking_key,
    valid_from,
    valid_to,
    next_valid_from
FROM ordered_versions
WHERE valid_to IS NOT NULL
  AND next_valid_from IS NOT NULL
  AND valid_to > next_valid_from
