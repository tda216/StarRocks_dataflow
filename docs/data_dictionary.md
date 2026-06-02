# Data Dictionary

## Dataset

- Dataset name: Hotel Booking Demand
- Expected file: `data/input/hotel_bookings.csv`
- Grain: one row per hotel booking record
- Source type: local CSV

## Why This Dataset Fits the Hospitality BI Case

Dataset này phù hợp cho Vinpearl / hospitality BI POC vì có các chiều phân tích gần với hotel booking reporting:

- hotel type
- arrival date
- country
- market segment
- distribution channel
- room type
- customer type
- lead time
- cancellation status
- ADR

Dataset không có real cost/expense, nên đây không phải true PNL dataset. Các revenue metrics là estimated/simulated từ `adr` và số đêm ở. Nếu có cost/margin thì phải label rõ là simulated.

## Source Columns

| Column | Meaning | Notes |
| --- | --- | --- |
| `hotel` | Hotel type | `City Hotel` hoặc `Resort Hotel`. |
| `is_canceled` | Cancellation flag | `1` nếu booking bị cancel, `0` nếu không. |
| `lead_time` | Days between booking date and arrival date | Dùng để tạo `lead_time_bucket`. |
| `arrival_date_year` | Arrival year | Dùng để tạo `arrival_date`. |
| `arrival_date_month` | Arrival month name | Cast thành month number trong staging. |
| `arrival_date_week_number` | Arrival week number | Source field. |
| `arrival_date_day_of_month` | Arrival day of month | Dùng để tạo `arrival_date`. |
| `stays_in_weekend_nights` | Weekend nights | Dùng tính `total_nights`. |
| `stays_in_week_nights` | Week nights | Dùng tính `total_nights`. |
| `adults` | Adult guest count | Dùng tính `total_guests`. |
| `children` | Child guest count | Có missing values nhỏ, được xử lý trong dbt. |
| `babies` | Baby guest count | Dùng tính `total_guests`. |
| `meal` | Meal package | Dimension phụ. |
| `country` | Guest country code | Missing được fill thành `Unknown`. |
| `market_segment` | Market segment | Ví dụ `Online TA`, `Groups`, `Corporate`. |
| `distribution_channel` | Booking distribution channel | Ví dụ `TA/TO`, `Direct`, `Corporate`. |
| `is_repeated_guest` | Repeat guest flag | `1` nếu repeated guest. |
| `previous_cancellations` | Previous cancellation count | Source behavior signal. |
| `previous_bookings_not_canceled` | Previous successful booking count | Source behavior signal. |
| `reserved_room_type` | Reserved room type code | Dùng cho room performance. |
| `assigned_room_type` | Assigned room type code | Dùng so sánh room assignment. |
| `booking_changes` | Booking change count | Source behavior signal. |
| `deposit_type` | Deposit type | Dùng trong cancellation analysis. |
| `agent` | Agent identifier | Missing nhiều. |
| `company` | Company identifier | Missing nhiều. |
| `days_in_waiting_list` | Waiting list days | Source operational field. |
| `customer_type` | Customer type | Ví dụ `Transient`, `Contract`, `Group`. |
| `adr` | Average Daily Rate | Dùng tính revenue. Negative ADR được đưa về `0` trong derived metrics. |
| `required_car_parking_spaces` | Required parking spaces | Source operational field. |
| `total_of_special_requests` | Special request count | Source behavior signal. |
| `reservation_status` | Final reservation status | Ví dụ `Check-Out`, `Canceled`, `No-Show`. |
| `reservation_status_date` | Status date | Cast thành DATE. |

## Raw Table

Raw table:

```text
hotel_booking.raw_hotel_bookings
```

Raw table giữ đa số source columns dạng `VARCHAR` để CSV loading robust. Type casting và cleaning thực hiện trong `stg_hotel_bookings`.

Metadata columns:

| Column | Meaning |
| --- | --- |
| `source_file` | Raw object path, ví dụ `s3://hotel-booking-raw/hotel_booking_demand/hotel_bookings.csv`. |
| `loaded_at` | Timestamp khi row được load vào StarRocks. |

## Cleaned Fields

| Field | Model | Definition |
| --- | --- | --- |
| `arrival_date` | `stg_hotel_bookings` | Date built from year, month name, and day of month. |
| `country` | `stg_hotel_bookings` | Missing country becomes `Unknown`. |
| `market_segment` | `stg_hotel_bookings` | Missing value becomes `Unknown`. |
| `distribution_channel` | `stg_hotel_bookings` | Missing value becomes `Unknown`. |
| `reserved_room_type` | `stg_hotel_bookings` | Missing value becomes `Unknown`. |
| `assigned_room_type` | `stg_hotel_bookings` | Missing value becomes `Unknown`. |
| `adr` | `int_booking_metrics` | Negative ADR is converted to `0` for metrics. Original value kept as `source_adr`. |
| `is_cancelled` | `int_booking_metrics` | Clean integer cancellation flag. |
| `guest_type` | `int_booking_metrics` | Derived as `solo`, `couple`, `family/group`, or `unknown`. |

## Derived Metrics

| Metric | Definition | Notes |
| --- | --- | --- |
| `total_nights` | `stays_in_weekend_nights + stays_in_week_nights` | Uses non-negative value. |
| `total_guests` | `adults + children + babies` | Missing children treated as `0`. |
| `estimated_revenue` | `adr * total_nights` | Main MVP revenue estimate. |
| `realized_revenue` | `estimated_revenue` when `is_cancelled = 0`, else `0` | Revenue after cancellation. |
| `cancellation_rate` | cancelled bookings / total bookings | Calculated in mart tables. |
| `average_adr` / `avg_adr` | average of cleaned `adr` | Name differs by mart table. |
| `lead_time_bucket` | bucket from `lead_time` | `0-7 days`, `8-30 days`, `31-90 days`, `91-180 days`, `180+ days`. |
| `stay_length_bucket` | bucket from `total_nights` | `0 night`, `1-2 nights`, `3-5 nights`, `6-10 nights`, `10+ nights`. |

## Mart Tables for Dashboard

Superset dashboard charts should use mart tables only:

- `mart_daily_booking_revenue`
- `mart_monthly_booking_revenue`
- `mart_hotel_performance`
- `mart_room_performance`
- `mart_market_segment_performance`
- `mart_channel_performance`
- `mart_country_performance`
- `mart_cancellation_analysis`
- `mart_lead_time_analysis`
- `mart_customer_type_performance`

Do not use raw, staging, intermediate, fact, or dimension tables for dashboard charts.

## Caveats

- This is not a real PNL dataset.
- No real cost, expense, margin, or profit exists in the source CSV.
- `estimated_revenue` and `realized_revenue` are derived from `adr * total_nights`.
- `mart_simulated_pnl` is optional and must be labelled simulated only.
- ADR outliers exist, including a very high max ADR and negative ADR in source data. Negative ADR is handled in dbt metrics by converting to `0`.
