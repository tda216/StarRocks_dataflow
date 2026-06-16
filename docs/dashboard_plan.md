# Dashboard Plan

Dashboard name:

```text
Hotel Booking BI Dashboard
```

Dashboard dùng StarRocks mart tables làm Superset datasets. Không dùng raw/staging/intermediate tables cho charts.

Local MVP có bootstrap script để tạo nhanh dashboard demo:

```bash
docker compose exec superset python /app/bootstrap_scripts/bootstrap_superset_dashboard.py
```

Manual UI vẫn dùng được để chỉnh layout, filter scope, format, hoặc thêm chart sau khi bootstrap.

## Dashboard Source Tables

Allowed mart datasets:

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

Do not use:

- `raw_hotel_bookings_history`
- `scd_hotel_bookings`
- `stg_hotel_bookings`
- `int_booking_metrics`
- `fact_bookings`
- `dim_*`

## Recommended Dashboard Structure

For demo readiness, one summary dashboard is enough:

```text
Hotel Booking BI Dashboard
```

Optional detail dashboards can exist:

- `Overview KPIs`
- `Time Trends`
- `Hotel / Room Performance`
- `Segment / Channel Analysis`
- `Country / Demand Analysis`
- `Cancellation / Lead Time Analysis`

## Native Filters

| Filter | Dataset | Column | Scope |
| --- | --- | --- | --- |
| Date range | `mart_daily_booking_revenue` | `arrival_date` | Daily trend charts only |
| Month range | `mart_monthly_booking_revenue` | `month_start_date` | Monthly trend charts only |
| Hotel | `mart_hotel_performance`, `mart_cancellation_analysis` | `hotel` | Hotel and cancellation charts with `hotel` |
| Market segment | `mart_market_segment_performance`, `mart_cancellation_analysis` | `market_segment` | Segment and cancellation charts |
| Distribution channel | `mart_channel_performance`, `mart_cancellation_analysis` | `distribution_channel` | Channel and cancellation charts |
| Country | `mart_country_performance` | `country` | Country charts only |
| Room type | `mart_room_performance` | `reserved_room_type` | Room charts only |
| Customer type | `mart_customer_type_performance` | `customer_type` | Customer type charts only |

Important: scope each filter only to charts whose dataset has that column.

## Chart Mapping

| Section | Chart Name | Superset Chart Type | Source Mart Table | Dimensions | Metrics | Filters | Business Question |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Overview KPIs | Total bookings | Big Number | `mart_hotel_performance` | none | `SUM(bookings)` | Hotel | Tổng số bookings là bao nhiêu? |
| Overview KPIs | Cancelled bookings | Big Number | `mart_hotel_performance` | none | `SUM(cancelled_bookings)` | Hotel | Có bao nhiêu bookings bị cancel? |
| Overview KPIs | Cancellation rate | Big Number | `mart_hotel_performance` | none | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | Hotel | Tỷ lệ cancel tổng quan là bao nhiêu? |
| Overview KPIs | Estimated revenue | Big Number | `mart_hotel_performance` | none | `SUM(estimated_revenue)` | Hotel | Revenue ước tính trước cancellation là bao nhiêu? |
| Overview KPIs | Realized revenue | Big Number | `mart_hotel_performance` | none | `SUM(realized_revenue)` | Hotel | Revenue sau cancellation là bao nhiêu? |
| Overview KPIs | Average ADR | Big Number | `mart_hotel_performance` | none | `AVG(avg_adr)` | Hotel | ADR trung bình là bao nhiêu? |
| Time Trends | Daily bookings trend | Line Chart | `mart_daily_booking_revenue` | `arrival_date` | `SUM(total_bookings)` | Date range | Demand thay đổi theo ngày như thế nào? |
| Time Trends | Monthly estimated revenue trend | Line Chart | `mart_monthly_booking_revenue` | `month_start_date` | `SUM(estimated_revenue)` | Month range | Revenue ước tính thay đổi theo tháng như thế nào? |
| Time Trends | Monthly cancellation rate trend | Line Chart | `mart_monthly_booking_revenue` | `month_start_date` | `SUM(cancelled_bookings) / NULLIF(SUM(total_bookings), 0)` | Month range | Cancellation rate thay đổi theo tháng như thế nào? |
| Hotel / Room Performance | Revenue by hotel type | Bar Chart | `mart_hotel_performance` | `hotel` | `SUM(realized_revenue)` | Hotel | Hotel type nào tạo realized revenue cao hơn? |
| Hotel / Room Performance | ADR by hotel type | Bar Chart | `mart_hotel_performance` | `hotel` | `AVG(avg_adr)` | Hotel | Hotel type nào có ADR cao hơn? |
| Hotel / Room Performance | Revenue by room type | Bar Chart | `mart_room_performance` | `reserved_room_type` | `SUM(realized_revenue)` | Room type | Room type nào tạo revenue cao hơn? |
| Hotel / Room Performance | Cancellation rate by room type | Bar Chart | `mart_room_performance` | `reserved_room_type` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | Room type | Room type nào có cancellation risk cao hơn? |
| Segment / Channel Analysis | Revenue by market segment | Bar Chart | `mart_market_segment_performance` | `market_segment` | `SUM(realized_revenue)` | Market segment | Segment nào đóng góp revenue nhiều nhất? |
| Segment / Channel Analysis | Booking count by distribution channel | Bar Chart | `mart_channel_performance` | `distribution_channel` | `SUM(bookings)` | Distribution channel | Channel nào tạo nhiều bookings nhất? |
| Segment / Channel Analysis | Cancellation rate by distribution channel | Bar Chart | `mart_channel_performance` | `distribution_channel` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | Distribution channel | Channel nào có cancellation rate cao nhất? |
| Segment / Channel Analysis | Customer type performance | Table | `mart_customer_type_performance` | `customer_type`, `guest_type` | `SUM(bookings)`, `SUM(realized_revenue)`, `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | Customer type | Customer type nào performance tốt hơn? |
| Country / Demand Analysis | Top countries by bookings | Bar Chart | `mart_country_performance` | `country` | `SUM(bookings)` | Country | Country nào có demand cao nhất? |
| Country / Demand Analysis | Top countries by realized revenue | Bar Chart | `mart_country_performance` | `country` | `SUM(realized_revenue)` | Country | Country nào đóng góp revenue cao nhất? |
| Cancellation / Lead Time Analysis | Cancellation rate by lead time bucket | Bar Chart | `mart_lead_time_analysis` | `lead_time_bucket` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | none | Booking xa/ngắn ngày ảnh hưởng cancellation thế nào? |
| Cancellation / Lead Time Analysis | Cancellation by segment/channel/deposit | Table or Heatmap | `mart_cancellation_analysis` | `market_segment`, `distribution_channel`, `deposit_type` | `SUM(total_bookings)`, `SUM(cancelled_bookings)`, `SUM(cancelled_bookings) / NULLIF(SUM(total_bookings), 0)` | Hotel, Market segment, Distribution channel | Segment/channel/deposit nào có cancellation risk cao? |

## Demo Recommendation

For final demo, do not overload one dashboard with every possible chart. A practical summary dashboard can use 10-12 charts:

- 4 KPI cards: bookings, cancellation rate, realized revenue, average ADR
- 2 time trends: daily bookings, monthly revenue
- 2 hotel/room charts: revenue by hotel type, revenue by room type
- 2 segment/channel charts: revenue by market segment, cancellation rate by distribution channel
- 1 country chart: top countries by bookings
- 1 cancellation chart: cancellation rate by lead time bucket

Detail dashboards can hold the remaining charts.

## Visual Polish Checklist

- Sort category bar charts descending by the main metric.
- Limit country charts to Top 10 or Top 15.
- Format rates as percent.
- Format revenue as compact currency, for example `$26.0M`.
- Add dashboard section headers with Markdown.
- Publish dashboards before demo so they do not show as Draft.
