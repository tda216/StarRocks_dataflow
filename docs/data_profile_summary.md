# Data Profile Summary

Generated at: 2026-06-14 14:49:07 UTC

- Dataset path: `data/input/hotel_bookings.csv`
- So dong (rows): 119,390
- So cot (columns): 32

## Columns

| Column | Inferred Type | Missing Values |
| --- | --- | ---: |
| `hotel` | string | 0 |
| `is_canceled` | integer | 0 |
| `lead_time` | integer | 0 |
| `arrival_date_year` | integer | 0 |
| `arrival_date_month` | string | 0 |
| `arrival_date_week_number` | integer | 0 |
| `arrival_date_day_of_month` | integer | 0 |
| `stays_in_weekend_nights` | integer | 0 |
| `stays_in_week_nights` | integer | 0 |
| `adults` | integer | 0 |
| `children` | integer | 4 |
| `babies` | integer | 0 |
| `meal` | string | 0 |
| `country` | string | 488 |
| `market_segment` | string | 0 |
| `distribution_channel` | string | 0 |
| `is_repeated_guest` | integer | 0 |
| `previous_cancellations` | integer | 0 |
| `previous_bookings_not_canceled` | integer | 0 |
| `reserved_room_type` | string | 0 |
| `assigned_room_type` | string | 0 |
| `booking_changes` | integer | 0 |
| `deposit_type` | string | 0 |
| `agent` | integer | 16,340 |
| `company` | integer | 112,593 |
| `days_in_waiting_list` | integer | 0 |
| `customer_type` | string | 0 |
| `adr` | decimal | 0 |
| `required_car_parking_spaces` | integer | 0 |
| `total_of_special_requests` | integer | 0 |
| `reservation_status` | string | 0 |
| `reservation_status_date` | date | 0 |

## ADR Checks

- Min ADR: -6.38
- Max ADR: 5,400.00
- Mean ADR: 101.83
- IQR lower bound: -15.77
- IQR upper bound: 211.06
- Possible ADR outliers: 3,793

## Derived Metric Checks

- Zero-night bookings: 715
- Zero-guest bookings: 180
- Rows skipped due to invalid metric fields: 0
- Estimated revenue total: 42,723,497.53
- Realized revenue total: 25,996,260.41

## Metric Definitions

- `total_nights = stays_in_weekend_nights + stays_in_week_nights`
- `total_guests = adults + children + babies`
- `estimated_revenue = adr * total_nights`
- `realized_revenue = estimated_revenue` only when `is_canceled = 0`, otherwise `0`
