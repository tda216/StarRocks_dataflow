# Final Validation Checklist

Checklist này dùng trước khi demo mentor.

## 1. Services

```bash
docker compose config
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

```bash
ls -lh data/input/hotel_bookings.csv
ls -lh data/input/incremental_batches/
```

Expected:

- original CSV exists at `data/input/hotel_bookings.csv`
- generated files include `batch_001_initial.csv` to `batch_005_reverted_state.csv`

## 3. Airflow DAG

```bash
docker compose exec airflow-webserver airflow dags unpause hotel_booking_pipeline
docker compose exec airflow-webserver airflow dags trigger hotel_booking_pipeline
```

Expected task groups:

```text
ingestion -> transformation -> validation
```

Expected success meaning:

- raw batch CSVs uploaded to MinIO
- Spark appended Bronze Iceberg raw history
- StarRocks External Catalog exists
- `dbt debug`, `dbt run`, and `dbt test` pass
- dbt-created mart views and Materialized Views have rows

## 4. MinIO Raw Objects

Open MinIO:

```text
http://localhost:9001
```

Check bucket:

```text
hotel-booking-raw
```

Expected raw layout:

```text
hotel_booking_demand/incremental_batches/
  etl_year=2026/
    etl_month=01/
      etl_day=01/
        raw_batch_sequence=001/
          batch_001_initial.csv
```

## 5. StarRocks And Iceberg

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW CATALOGS;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES FROM iceberg_catalog;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM iceberg_catalog.hotel_booking_lakehouse;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW MATERIALIZED VIEWS FROM hotel_booking;"
```

Expected:

- `iceberg_catalog` exists
- `iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history` exists
- dbt views exist in `hotel_booking`
- MVs exist: `mv_daily_booking_revenue`, `mv_monthly_booking_revenue`, `mv_hotel_performance`

Bronze row count by batch:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT batch_id, COUNT(*) AS row_count
FROM iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history
GROUP BY batch_id
ORDER BY batch_id;
"
```

Expected batches:

- `batch_001_initial`
- `batch_002_updates`
- `batch_003_duplicate_replay`
- `batch_004_same_state`
- `batch_005_reverted_state`

## 6. dbt Validation

Manual dbt run if needed:

```bash
docker compose exec airflow-webserver dbt run \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking \
  --no-partial-parse \
  --threads 1

docker compose exec airflow-webserver dbt test \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking \
  --no-partial-parse \
  --threads 1
```

Hard-fail tests include:

- one business state per `booking_key + batch_id`
- one current row per `booking_key`
- current row count matches distinct `booking_key`
- non-negative metrics
- mart/MV objects are not empty
- MV totals match equivalent mart views

Profile-style warning tests include:

- ADR outliers / negative source ADR
- zero-night bookings
- zero-guest bookings
- missing or unknown core dimensions
- invalid arrival-date components

Warnings are acceptable for demo if explained as data quality observations.

## 7. Current-State Checks

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT
    (SELECT COUNT(*) FROM hotel_booking.int_current_hotel_bookings) AS current_rows,
    (SELECT COUNT(DISTINCT booking_key) FROM hotel_booking.stg_iceberg_raw_hotel_bookings) AS distinct_booking_keys;
"
```

Expected: two counts match.

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, batch_id, COUNT(DISTINCT record_hash) AS hash_count
FROM hotel_booking.stg_iceberg_raw_hotel_bookings
GROUP BY booking_key, batch_id
HAVING COUNT(DISTINCT record_hash) > 1;
"
```

Expected: no rows.

## 8. Materialized Views

MVs are created/refreshed by dbt during `dbt run`; there is no separate MV apply script in the main flow.

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SHOW MATERIALIZED VIEWS FROM hotel_booking;
"
```

Demo query rewrite:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/demo_readiness.py
```

Expected:

- MVs exist and are active
- MV aggregate totals match mart views
- query rewrite demo can show `mv_daily_booking_revenue` in the `EXPLAIN` plan when StarRocks rewrite applies

## 9. Superset

Open:

```text
http://localhost:8088
```

Connection URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Dashboard charts should use only mart objects:

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

## 10. Demo Notes

Be ready to explain:

- Spark is ingestion-only in this refactor.
- Bronze Iceberg stores append-only raw history.
- dbt owns dedup/current/metrics/fact/dim/mart logic.
- `record_hash` is computed in dbt, not Spark.
- Superset queries mart views only.
- MVs are StarRocks optimization objects, not the business source of truth.
- This MVP is batch-only and not true PNL.
