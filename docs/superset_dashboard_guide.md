# Superset Dashboard Guide

Guide này hướng dẫn tạo dashboard Superset cho local MVP:

```text
StarRocks mart tables -> Superset datasets -> Hotel Booking BI Dashboard
```

Không dùng raw/staging/intermediate tables cho dashboard charts.

Khuyến nghị cho demo: dùng bootstrap script để tạo nhanh database connection, datasets, charts, và dashboard. Sau đó chỉ chỉnh layout/filter thủ công nếu cần.

## 1. Mở Superset

Start stack nếu chưa chạy:

```bash
docker compose up -d --build
```

Mở Superset:

```text
http://localhost:8088
```

Default login:

```text
admin / admin
```

## 2. Tạo Dashboard Tự Động

Điều kiện trước khi chạy:

- Airflow DAG hoặc dbt manual run đã tạo mart tables trong StarRocks.
- Superset service đang chạy.
- Nếu vừa thêm mount `scripts/` vào Superset trong `docker-compose.yml`, recreate Superset một lần:

```bash
docker compose up -d superset
```

Chạy bootstrap script:

```bash
docker compose exec superset python /app/bootstrap_scripts/bootstrap_superset_dashboard.py
```

Script sẽ tạo/update idempotent:

- Database connection: `StarRocks Hotel Booking`
- 10 Superset datasets từ mart tables
- 13 demo charts
- Dashboard: `Hotel Booking BI Dashboard`

Nếu container Superset chưa được recreate nên chưa thấy `/app/bootstrap_scripts`, dùng fallback:

```bash
docker compose cp scripts/bootstrap_superset_dashboard.py superset:/tmp/bootstrap_superset_dashboard.py
docker compose exec superset python /tmp/bootstrap_superset_dashboard.py
```

Sau khi chạy xong, mở:

```text
http://localhost:8088/superset/dashboard/hotel-booking-bi-dashboard/
```

Bootstrap script chỉ dùng mart tables. Không tạo chart từ raw/staging/intermediate tables.

## 3. Verify StarRocks SQLAlchemy Support

Superset image installs the StarRocks SQLAlchemy package and MySQL client from `docker/superset/requirements.txt`. Do not assume a version from the docs; verify the actual installed versions in the container.

Kiểm tra trong container:

```bash
docker compose exec superset python - <<'PY'
import importlib.metadata as md
for pkg in ["starrocks", "mysqlclient"]:
    print(f"{pkg}=={md.version(pkg)}")
PY
```

Nếu package thiếu, rebuild Superset image:

```bash
docker compose build superset
docker compose up -d superset
```

## 4. Tạo StarRocks Database Connection Thủ Công

Nếu không dùng bootstrap script, tạo thủ công trong Superset UI:

1. Vào `Settings` -> `Database Connections`.
2. Chọn `+ Database`.
3. Chọn nhập SQLAlchemy URI thủ công.
4. Dùng URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Giải thích:

- `starrocks`: Docker Compose service name trong `data_network`.
- `9030`: StarRocks query port.
- `default_catalog.hotel_booking`: catalog và database chứa dbt mart tables.

Test connection. Nếu pass, lưu connection với tên:

```text
StarRocks Hotel Booking
```

## 5. Tạo Superset Datasets Thủ Công

Chỉ tạo datasets từ mart tables sau:

| Dataset Name | StarRocks Table |
| --- | --- |
| `mart_daily_booking_revenue` | `hotel_booking.mart_daily_booking_revenue` |
| `mart_monthly_booking_revenue` | `hotel_booking.mart_monthly_booking_revenue` |
| `mart_hotel_performance` | `hotel_booking.mart_hotel_performance` |
| `mart_room_performance` | `hotel_booking.mart_room_performance` |
| `mart_market_segment_performance` | `hotel_booking.mart_market_segment_performance` |
| `mart_channel_performance` | `hotel_booking.mart_channel_performance` |
| `mart_country_performance` | `hotel_booking.mart_country_performance` |
| `mart_cancellation_analysis` | `hotel_booking.mart_cancellation_analysis` |
| `mart_lead_time_analysis` | `hotel_booking.mart_lead_time_analysis` |
| `mart_customer_type_performance` | `hotel_booking.mart_customer_type_performance` |

Trong Superset UI:

1. Vào `Datasets`.
2. Chọn `+ Dataset`.
3. Chọn database `StarRocks Hotel Booking`.
4. Schema/database: `hotel_booking`.
5. Chọn mart table.
6. Save.

Không tạo dataset từ:

- `raw_hotel_bookings_history`
- `scd_hotel_bookings`
- `int_current_hotel_bookings`
- `int_booking_metrics`
- `fact_bookings`
- `dim_*`

Ngoại lệ: `fact_bookings` chỉ dùng để validation/debug, không dùng làm dashboard chart source trong MVP này.

## 6. Dashboard

Dashboard name:

```text
Hotel Booking BI Dashboard
```

Recommended layout:

1. Overview KPIs
2. Time Trends
3. Hotel / Room Performance
4. Segment / Channel Analysis
5. Country / Demand Analysis
6. Cancellation / Lead Time Analysis

## 7. Native Filters

Tạo native filters sau ở dashboard level:

| Filter | Source Dataset | Column |
| --- | --- | --- |
| Date range | `mart_daily_booking_revenue` | `arrival_date` |
| Hotel | `mart_hotel_performance`, `mart_cancellation_analysis` | `hotel` |
| Market segment | `mart_market_segment_performance`, `mart_cancellation_analysis` | `market_segment` |
| Distribution channel | `mart_channel_performance`, `mart_cancellation_analysis` | `distribution_channel` |
| Country | `mart_country_performance` | `country` |
| Room type | `mart_room_performance` | `reserved_room_type` |
| Customer type | `mart_customer_type_performance` | `customer_type` |

Filter scope nên chỉ apply vào charts có column tương ứng. Ví dụ `Country` filter không nên apply vào `mart_monthly_booking_revenue` nếu mart đó không có `country`.

### Filter Setup Example

Trong dashboard, chọn `Edit dashboard` -> `Filters` -> `Add filters and dividers`.

Hotel filter:

- Filter type: `Value`
- Filter name: `Hotel`
- Dataset: `mart_hotel_performance`
- Column: `hotel`
- Can select multiple values: checked
- Scoping: chỉ apply cho charts dùng `mart_hotel_performance`, ví dụ KPI cards, `Revenue by hotel type`, `ADR by hotel type`.

Date range filter:

- Filter type: `Time range`
- Filter name: `Date range`
- Dataset: `mart_daily_booking_revenue`
- Column: `arrival_date`
- Scoping: apply cho `Daily bookings trend`.

Country filter:

- Filter type: `Value`
- Filter name: `Country`
- Dataset: `mart_country_performance`
- Column: `country`
- Can select multiple values: checked
- Dynamically search all filter values: checked
- Scoping: apply cho country charts only.

Room type filter:

- Filter type: `Value`
- Filter name: `Room type`
- Dataset: `mart_room_performance`
- Column: `reserved_room_type`
- Scoping: apply cho room performance charts only.

Không dùng `Apply to all charts` cho mọi filter. Nhiều mart không có cùng column, nên scope sai có thể làm filter không có tác dụng hoặc chart lỗi.

## 7. Chart-by-Chart Build Guide

### A. Overview KPIs

| Chart | Dataset | Chart Type | Metric | Dimension |
| --- | --- | --- | --- | --- |
| Total bookings | `mart_hotel_performance` | Big Number | `SUM(bookings)` | none |
| Cancelled bookings | `mart_hotel_performance` | Big Number | `SUM(cancelled_bookings)` | none |
| Cancellation rate | `mart_hotel_performance` | Big Number | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` | none |
| Estimated revenue | `mart_hotel_performance` | Big Number | `SUM(estimated_revenue)` | none |
| Realized revenue | `mart_hotel_performance` | Big Number | `SUM(realized_revenue)` | none |
| Average ADR | `mart_hotel_performance` | Big Number | `AVG(avg_adr)` | none |

Nếu Superset không nhận calculated metric trực tiếp, tạo metric custom bằng SQL expression trong chart:

```sql
SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)
```

### B. Time Trends

| Chart | Dataset | Chart Type | Time Column | Metrics |
| --- | --- | --- | --- | --- |
| Daily bookings trend | `mart_daily_booking_revenue` | Line Chart | `arrival_date` | `SUM(total_bookings)` |
| Monthly estimated revenue trend | `mart_monthly_booking_revenue` | Line Chart | `month_start_date` | `SUM(estimated_revenue)` |
| Monthly cancellation rate trend | `mart_monthly_booking_revenue` | Line Chart | `month_start_date` | `SUM(cancelled_bookings) / NULLIF(SUM(total_bookings), 0)` |

### C. Hotel / Room Performance

| Chart | Dataset | Chart Type | Dimension | Metrics |
| --- | --- | --- | --- | --- |
| Revenue by hotel type | `mart_hotel_performance` | Bar Chart | `hotel` | `SUM(realized_revenue)` |
| ADR by hotel type | `mart_hotel_performance` | Bar Chart | `hotel` | `AVG(avg_adr)` |
| Revenue by room type | `mart_room_performance` | Bar Chart | `reserved_room_type` | `SUM(realized_revenue)` |
| Cancellation rate by room type | `mart_room_performance` | Bar Chart | `reserved_room_type` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` |

### D. Segment / Channel Analysis

| Chart | Dataset | Chart Type | Dimension | Metrics |
| --- | --- | --- | --- | --- |
| Revenue by market segment | `mart_market_segment_performance` | Bar Chart | `market_segment` | `SUM(realized_revenue)` |
| Booking count by distribution channel | `mart_channel_performance` | Bar Chart | `distribution_channel` | `SUM(bookings)` |
| Cancellation rate by distribution channel | `mart_channel_performance` | Bar Chart | `distribution_channel` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` |
| Customer type performance | `mart_customer_type_performance` | Table | `customer_type`, `guest_type` | `SUM(bookings)`, `SUM(realized_revenue)`, `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` |

### E. Country / Demand Analysis

| Chart | Dataset | Chart Type | Dimension | Metrics |
| --- | --- | --- | --- | --- |
| Top countries by bookings | `mart_country_performance` | Bar Chart | `country` | `SUM(bookings)` |
| Top countries by realized revenue | `mart_country_performance` | Bar Chart | `country` | `SUM(realized_revenue)` |

Limit top country charts to top 10 or top 20.

### F. Cancellation / Lead Time Analysis

| Chart | Dataset | Chart Type | Dimension | Metrics |
| --- | --- | --- | --- | --- |
| Cancellation rate by lead time bucket | `mart_lead_time_analysis` | Bar Chart | `lead_time_bucket` | `SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)` |
| Cancellation by market segment/channel/deposit type | `mart_cancellation_analysis` | Table or Heatmap | `market_segment`, `distribution_channel`, `deposit_type` | `SUM(total_bookings)`, `SUM(cancelled_bookings)`, `SUM(cancelled_bookings) / NULLIF(SUM(total_bookings), 0)` |
| Booking distribution by stay length | `mart_lead_time_analysis` | Bar Chart | `stay_length_bucket` if available | `SUM(bookings)` |

Nếu `stay_length_bucket` chưa có trong mart hiện tại, bỏ chart này khỏi MVP hoặc tạo chart thay thế bằng `lead_time_bucket`.

## 8. Validation Queries

Chạy sau Airflow DAG/dbt success:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
```

Check mart row counts:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT 'mart_daily_booking_revenue' AS table_name, COUNT(*) AS row_count FROM hotel_booking.mart_daily_booking_revenue
UNION ALL
SELECT 'mart_monthly_booking_revenue', COUNT(*) FROM hotel_booking.mart_monthly_booking_revenue
UNION ALL
SELECT 'mart_hotel_performance', COUNT(*) FROM hotel_booking.mart_hotel_performance
UNION ALL
SELECT 'mart_room_performance', COUNT(*) FROM hotel_booking.mart_room_performance
UNION ALL
SELECT 'mart_market_segment_performance', COUNT(*) FROM hotel_booking.mart_market_segment_performance
UNION ALL
SELECT 'mart_channel_performance', COUNT(*) FROM hotel_booking.mart_channel_performance
UNION ALL
SELECT 'mart_country_performance', COUNT(*) FROM hotel_booking.mart_country_performance
UNION ALL
SELECT 'mart_cancellation_analysis', COUNT(*) FROM hotel_booking.mart_cancellation_analysis
UNION ALL
SELECT 'mart_lead_time_analysis', COUNT(*) FROM hotel_booking.mart_lead_time_analysis
UNION ALL
SELECT 'mart_customer_type_performance', COUNT(*) FROM hotel_booking.mart_customer_type_performance;
"
```

Check sample KPI:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT
  SUM(bookings) AS bookings,
  SUM(cancelled_bookings) AS cancelled_bookings,
  SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0) AS cancellation_rate,
  SUM(estimated_revenue) AS estimated_revenue,
  SUM(realized_revenue) AS realized_revenue,
  AVG(avg_adr) AS avg_adr
FROM hotel_booking.mart_hotel_performance;
"
```

## 9. Troubleshooting

### Connection test fail

Check Superset and StarRocks are on same Docker network:

```bash
docker compose ps superset starrocks
docker network inspect data_network
```

Check StarRocks query port:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT 1;"
```

Use URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Do not use `localhost` inside Superset, because Superset runs in a container. Use service name `starrocks`.

### Dataset list không thấy mart tables

Run Airflow DAG hoặc dbt:

```bash
docker compose exec airflow-webserver dbt run \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking
```

Then check:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
```

### Chart fail vì missing column

Một số filters không apply được cho mọi mart. Ví dụ:

- `country` chỉ có trong `mart_country_performance`.
- `reserved_room_type` chủ yếu dùng với `mart_room_performance`.
- `arrival_date` chỉ dùng với daily mart, `month_start_date` dùng với monthly mart.

Điều chỉnh native filter scope theo chart/dataset tương thích.

### Dashboard metadata mất sau restart

`docker compose down` giữ Superset volume.

`docker compose down -v` xóa Superset metadata volume, dashboard sẽ mất và cần tạo lại.
