# Data Dictionary

## Dataset

- Dataset name: Hotel Booking Demand
- Expected file: `data/input/hotel_bookings.csv`
- Grain: one row per hotel booking record
- Source type: local CSV converted into deterministic incremental batch CSVs for the MVP demo

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

## Synthetic Batch Metadata

Generated batch files are written to:

```text
data/input/incremental_batches/
```

Stable key:

```text
booking_key = source_dataset + ':' + original_source_row_number
```

`booking_key` is generated once by `scripts/generate_synthetic_batches.py` and persisted in every generated batch. Spark must not regenerate it from row number.

Metadata columns:

| Column | Meaning |
| --- | --- |
| `source_dataset` | Logical dataset name, currently `hotel_booking_demand`. |
| `original_source_row_number` | Stable row number from the original Kaggle CSV. |
| `booking_key` | Stable synthetic key for this POC. |
| `batch_id` | Deterministic batch identifier, for example `batch_001_initial`. |
| `batch_sequence` | Numeric batch order for SCD2. |
| `batch_effective_at` | Deterministic effective timestamp used as SCD2 `valid_from`. |
| `batch_row_number` | Row order inside the generated batch file. |
| `synthetic_operation` | Demo label such as `initial`, `update`, `duplicate_replay`, or fixture operation. |

## Iceberg Raw History

Iceberg table:

```text
iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history
```

Spark appends generated batch CSVs to this Iceberg table. Iceberg manages the table and stores data in Parquet under the MinIO `warehouse` bucket.

Additional ingestion metadata:

| Column | Meaning |
| --- | --- |
| `source_file_name` | Generated batch CSV filename. |
| `source_object_path` | Raw MinIO object path for the batch file. |
| `file_hash` | SHA-256 hash of the local batch file content. |
| `record_hash` | SHA-256 hash of normalized business columns only. |
| `ingested_at` | Physical ingestion timestamp. Not used for SCD2 validity. |
| `row_ingestion_id` | Deterministic row ingestion identifier. |

`record_hash` excludes ingestion metadata and derived metrics. It is used by dbt to detect business changes.

## SCD2 Fields

SCD2 table:

```text
hotel_booking.scd_hotel_bookings
```

| Column | Meaning |
| --- | --- |
| `record_hash` | Business-only hash used consistently across raw history, dedup, SCD2, current, and fact models. |
| `valid_from` | Equal to deterministic `batch_effective_at`. |
| `valid_to` | Next change record `valid_from`; `NULL` for current record. |
| `is_current` | `1` for latest version per `booking_key`, else `0`. |
| `first_seen_batch_id` | Batch that produced the SCD2 version. |
| `first_seen_batch_sequence` | Sequence of the batch that produced the SCD2 version. |

## Cleaned Fields

| Field | Model | Definition |
| --- | --- | --- |
| `arrival_date` | `int_current_hotel_bookings` | Date built from year, month name, and day of month. |
| `country` | `int_current_hotel_bookings` | Missing country becomes `Unknown`. |
| `market_segment` | `int_current_hotel_bookings` | Missing value becomes `Unknown`. |
| `distribution_channel` | `int_current_hotel_bookings` | Missing value becomes `Unknown`. |
| `reserved_room_type` | `int_current_hotel_bookings` | Missing value becomes `Unknown`. |
| `assigned_room_type` | `int_current_hotel_bookings` | Missing value becomes `Unknown`. |
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
| `cancellation_rate` | cancelled bookings / total bookings | For aggregated dashboards, use weighted formula `SUM(cancelled_bookings) / SUM(bookings)` or `SUM(cancelled_bookings) / SUM(total_bookings)`, not `AVG(cancellation_rate)`. |
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
