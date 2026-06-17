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
- `iceberg-rest`
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

`docs/data_profile_summary.md` profiles only the original CSV at `data/input/hotel_bookings.csv`. It does not profile generated incremental batches or Iceberg history.

## 3. MinIO Raw Storage

Open MinIO:

```text
http://localhost:9001
```

Expected bucket/object:

```text
hotel-booking-raw/hotel_booking_demand/incremental_batches/batch_001_initial.csv
hotel-booking-raw/hotel_booking_demand/incremental_batches/batch_002_updates.csv
hotel-booking-raw/hotel_booking_demand/incremental_batches/batch_003_duplicate_replay.csv
hotel-booking-raw/hotel_booking_demand/incremental_batches/batch_004_same_state.csv
hotel-booking-raw/hotel_booking_demand/incremental_batches/batch_005_reverted_state.csv
warehouse/
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

Expected task groups:

```text
precheck -> ingestion -> transformation -> optimization -> validation
```

Expected final task:

```text
validation.log_validation_counts
```

The DAG should finish successfully.

## 5. StarRocks Catalogs and Iceberg Raw History

Check database:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES;"
```

Check tables:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
```

Check external catalog:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW CATALOGS;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES FROM iceberg_catalog;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM iceberg_catalog.hotel_booking_lakehouse;"
```

Expected catalog:

```text
iceberg_catalog
```

Check Iceberg raw history by batch:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT batch_id, COUNT(*) AS row_count
FROM iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history
GROUP BY batch_id
ORDER BY batch_id;
"
```

Expected batch row counts:

```text
batch_001_initial             119390
batch_002_updates             17
batch_003_duplicate_replay    15
batch_004_same_state          1
batch_005_reverted_state      1
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

- `scd_hotel_bookings`
- `int_current_hotel_bookings`
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

- `fact_bookings = 119395` with the default generated batches: original `119390` rows plus `5` synthetic new rows.
- `fact_bookings` should not inflate after duplicate replay.
- mart tables have row count greater than `0`

Check SCD2 fixtures:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, COUNT(*) AS version_count
FROM hotel_booking.scd_hotel_bookings
WHERE booking_key IN ('hotel_booking_demand:1', 'hotel_booking_demand:2')
GROUP BY booking_key
ORDER BY booking_key;
"
```

Expected:

```text
hotel_booking_demand:1    1
hotel_booking_demand:2    3
```

Check no duplicate current records:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, COUNT(*) AS current_count
FROM hotel_booking.scd_hotel_bookings
WHERE is_current = 1
GROUP BY booking_key
HAVING COUNT(*) > 1;
"
```

Expected: no rows.

Check no multiple business states in the same batch for one `booking_key`:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, batch_id, COUNT(DISTINCT record_hash) AS hash_count
FROM hotel_booking.stg_iceberg_raw_hotel_bookings
GROUP BY booking_key, batch_id
HAVING COUNT(DISTINCT record_hash) > 1;
"
```

Expected: no rows.

Check no overlapping SCD2 periods:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT a.booking_key
FROM hotel_booking.scd_hotel_bookings a
JOIN hotel_booking.scd_hotel_bookings b
  ON a.booking_key = b.booking_key
 AND a.valid_from < COALESCE(b.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND b.valid_from < COALESCE(a.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND a.valid_from <> b.valid_from
LIMIT 10;
"
```

Expected: no rows.

## 8. StarRocks Materialized Views

Materialized Views run in the main Airflow DAG after `dbt_test`. Run the script manually only when testing this layer directly:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/apply_starrocks_materialized_views.py
```

Expected MVs:

- `mv_daily_booking_revenue`
- `mv_monthly_booking_revenue`
- `mv_hotel_performance`

Check MV status:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW MATERIALIZED VIEWS FROM hotel_booking;"
```

Expected:

- `is_active = true`
- `query_rewrite_status = VALID`
- validation script prints `diff_value = 0` for daily, monthly, and hotel checks

The DAG also validates query rewrite with `EXPLAIN`. A matching daily aggregate query written against `fact_bookings` should scan `mv_daily_booking_revenue`.

Superset uses dbt mart tables by default. MVs are the StarRocks optimization layer; dbt marts remain the business source of truth.

## 9. Superset

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

## 10. Demo Readiness

Before demo:

- Dashboard is saved/published, not left as Draft if possible.
- KPI cards render without SQL errors.
- Trend charts render.
- Country charts are sorted and limited to Top 10 or Top 15.
- Cancellation rates are formatted as percentages.
- Native filters have correct scope.
- Explain that `estimated_revenue` and `realized_revenue` are not true PNL.
- Explain that realtime, semantic layer, Cube.dev, and Agentic AI are out of scope for this MVP.
- Explain that Materialized Views are created in the Airflow optimization step and validate StarRocks query rewrite, while dbt marts remain the source of truth.

## 11. Troubleshooting Quick Checks

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
