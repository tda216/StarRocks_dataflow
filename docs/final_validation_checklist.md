# Final Validation Checklist

Use checklist này trước khi demo final local BI POC.

## 1. Docker Services

Start services:

```bash
docker compose up -d
```

Check status:

```bash
docker compose ps
```

Expected services:

- `minio`
- `starrocks`
- `airflow-postgres`
- `airflow-webserver`
- `airflow-scheduler`
- `superset`

## 2. Dataset

CSV exists:

```bash
test -f data/input/hotel_bookings.csv && echo "CSV exists"
```

Expected path:

```text
data/input/hotel_bookings.csv
```

Expected row count for current dataset:

```text
119390
```

## 3. MinIO Raw Storage

Open MinIO:

```text
http://localhost:9001
```

Expected bucket/object:

```text
hotel-booking-raw/hotel_booking_demand/hotel_bookings.csv
```

## 4. Airflow DAG

Open Airflow:

```text
http://localhost:8080
```

Trigger DAG manually:

```bash
docker compose exec airflow-webserver airflow dags unpause hotel_booking_pipeline
docker compose exec airflow-webserver airflow dags trigger hotel_booking_pipeline
```

Expected DAG:

```text
hotel_booking_pipeline
```

Expected final task:

```text
validation.log_mart_row_counts
```

The DAG should finish successfully.

## 5. StarRocks Database and Raw Row Count

Check database:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES;"
```

Check tables:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
```

Check raw row count:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT COUNT(*) FROM hotel_booking.raw_hotel_bookings;"
```

Expected:

```text
119390
```

## 6. dbt Run and Test

Run manually if needed:

```bash
docker compose exec airflow-webserver dbt run \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking
```

```bash
docker compose exec airflow-webserver dbt test \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking
```

Expected:

- `dbt run` succeeds.
- `dbt test` succeeds.

## 7. Fact, Dimension, and Mart Tables

Check key tables:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SHOW TABLES FROM hotel_booking;
"
```

Expected table groups:

- `raw_hotel_bookings`
- `stg_hotel_bookings`
- `int_booking_metrics`
- `dim_*`
- `fact_bookings`
- `mart_*`

Check mart row counts:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT 'fact_bookings' AS table_name, COUNT(*) AS row_count FROM hotel_booking.fact_bookings
UNION ALL
SELECT 'mart_daily_booking_revenue', COUNT(*) FROM hotel_booking.mart_daily_booking_revenue
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

Expected:

- `fact_bookings = 119390`
- mart tables have row count greater than `0`

## 8. Superset

Open Superset:

```text
http://localhost:8088
```

Expected connection URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Expected datasets:

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

Expected dashboard:

```text
Hotel Booking BI Dashboard
```

Dashboard charts must query mart datasets only.

## 9. Demo Readiness

Before demo:

- Dashboard is saved/published, not left as Draft if possible.
- KPI cards render without SQL errors.
- Trend charts render.
- Country charts are sorted and limited to Top 10 or Top 15.
- Cancellation rates are formatted as percentages.
- Native filters have correct scope.
- Explain that `estimated_revenue` and `realized_revenue` are not true PNL.
- Explain that realtime, semantic layer, Cube.dev, and Agentic AI are out of scope for this MVP.

## 10. Troubleshooting Quick Checks

Service logs:

```bash
docker compose logs -f starrocks
docker compose logs -f airflow-scheduler
docker compose logs -f superset
```

StarRocks connection:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT 1;"
```

Superset driver:

```bash
docker compose exec superset python - <<'PY'
import importlib.metadata as md
for pkg in ["starrocks", "mysqlclient"]:
    print(f"{pkg}=={md.version(pkg)}")
PY
```
