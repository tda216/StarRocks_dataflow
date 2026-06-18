# StarRocks Dataflow

Local BI POC cho hotel booking analytics. Project mô phỏng một flow BI đơn giản theo tinh thần architecture production, nhưng chạy được trên laptop.

## Mục đích POC

Production-like flow trong dự án gốc có thể dùng Redshift / ClickHouse cho warehouse và serving. Trong POC này, StarRocks đóng vai trò cả warehouse và serving layer để validate local MVP:

```text
CSV -> MinIO raw -> Bronze Iceberg history -> Silver Iceberg tables -> StarRocks/dbt Gold marts -> StarRocks MVs -> Superset dashboard
```

Đây không phải performance benchmark chính thức. MVP hiện tại không bao gồm realtime/streaming, Cube.dev, semantic layer, hoặc Agentic AI.

## MVP Scope

Included:

- Generate synthetic incremental batch CSVs từ local source CSV.
- Upload immutable batch CSV files vào MinIO raw bucket.
- Dùng Spark technical pre-transform để append Bronze raw history vào Iceberg.
- Dùng Spark build Silver Iceberg tables cho dedup, SCD2/version history, current state, and booking metrics.
- Query Iceberg Bronze/Silver tables qua StarRocks external catalog.
- Dùng dbt qua StarRocks để expose intermediate views, run tests, and materialize Gold fact, dimension, and mart tables.
- Tạo và validate StarRocks Materialized Views cho một số aggregation serving/query rewrite.
- Orchestrate batch pipeline bằng Airflow manual DAG.
- Build Superset dashboard từ StarRocks mart tables.

Not included:

- Realtime ingestion.
- Cube.dev / semantic layer.
- Agentic AI.
- Real PNL, real cost, or real profit calculation.
- Formal performance benchmark.

## Tech Stack

| Tool | Role |
| --- | --- |
| Docker Compose | Local service orchestration |
| MinIO | Raw object storage |
| Iceberg REST Catalog | Lakehouse catalog cho Bronze raw history và Silver tables |
| Spark / PySpark | Technical pre-transform, Iceberg append-only ingestion, and Silver table build |
| StarRocks | External catalog reader + internal warehouse/serving layer |
| StarRocks Materialized View | Precomputed aggregation and query rewrite validation |
| dbt | SQL transformation and mart creation |
| Airflow | Batch orchestration |
| Superset | BI dashboard |

## Architecture

```text
data/input/hotel_bookings.csv
  -> generated incremental batch CSVs
  -> MinIO bucket hotel-booking-raw
  -> Bronze Iceberg raw history in MinIO warehouse bucket
  -> Silver Iceberg dedup/version/current/metrics tables
  -> StarRocks external catalog iceberg_catalog
  -> dbt views over Iceberg Silver tables
  -> StarRocks internal Gold fact/dim/mart serving tables
  -> StarRocks Materialized Views for selected aggregations
  -> Superset datasets and dashboard
```

Important: Spark handles technical Bronze/Silver lakehouse processing. dbt runs through StarRocks, reads Iceberg through the StarRocks external catalog, exposes intermediate views, and materializes Gold serving tables inside StarRocks. dbt does not transform files directly in MinIO.

More detail: [docs/architecture.md](docs/architecture.md)

## Local Resource Notes

StarRocks + Airflow + Superset can be memory-heavy.

Recommended:

- 16GB RAM preferred.
- 8GB RAM possible with conservative Docker limits.
- Increase Docker Desktop memory if containers restart due to OOM.
- Stop optional services when testing one layer only.
- Keep dbt threads low, for example `1` or `2`.
- Airflow runs local PySpark ingestion for this MVP, so the Airflow scheduler can use more memory during ingestion.

Resource limits in `docker-compose.yml` are intentionally conservative for local development.

## Dataset

Place Kaggle Hotel Booking Demand CSV here:

```text
data/input/hotel_bookings.csv
```

Run lightweight profile:

```bash
python3 scripts/profile_dataset.py
```

Generated summary for the original Kaggle CSV only:

```text
docs/data_profile_summary.md
```

This profile does not describe generated incremental batches or Iceberg history. Those are validated later by batch/SCD2 checks.

Data dictionary: [docs/data_dictionary.md](docs/data_dictionary.md)

## Start Services

Create `.env` if needed:

```bash
cp .env.example .env
```

Validate Compose:

```bash
docker compose config
```

Start stack:

```bash
docker compose up -d --build
```

Check services:

```bash
docker compose ps
```

Stop services but keep volumes:

```bash
docker compose down
```

Reset all local state, including MinIO, StarRocks, Airflow metadata, and Superset dashboards:

```bash
docker compose down -v
```

## Open UIs

| UI | URL | Default Login |
| --- | --- | --- |
| MinIO | `http://localhost:9001` | `minioadmin / minioadmin` |
| Airflow | `http://localhost:8080` | `admin / admin` |
| Superset | `http://localhost:8088` | `admin / admin` |
| StarRocks FE | `http://localhost:8030` if available | no login configured for local MVP |

StarRocks SQL check:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT 1;"
```

## Run the Pipeline with Airflow

DAG name:

```text
hotel_booking_pipeline
```

Trigger manually:

```bash
docker compose exec airflow-webserver airflow dags unpause hotel_booking_pipeline
docker compose exec airflow-webserver airflow dags trigger hotel_booking_pipeline
```

Task groups:

```text
precheck
  -> ingestion
  -> transformation
  -> optimization
  -> validation
```

The DAG performs:

- check CSV exists
- profile dataset
- generate synthetic incremental batches
- upload batch CSVs to MinIO
- append batch history to Iceberg with Spark
- wait for StarRocks
- create StarRocks database and Iceberg external catalog
- validate Iceberg raw history row count by batch
- build Silver Iceberg tables for dedup, SCD2/version history, current state, and booking metrics
- validate Silver Iceberg tables through StarRocks external catalog
- run dbt debug/run/test
- create, refresh, and validate StarRocks Materialized Views
- validate StarRocks query rewrite to `mv_daily_booking_revenue`
- log version/current/fact/mart row counts

Repeated runs are idempotent at the Iceberg ingestion step: a previously ingested `batch_id` is skipped. dbt also compresses consecutive unchanged business states so duplicate replay does not inflate current/fact/mart counts.

## Manual Batch and Iceberg Load if Needed

Install script dependencies:

```bash
python3 -m pip install -r scripts/requirements.txt
```

Generate deterministic incremental batches:

```bash
python3 scripts/generate_synthetic_batches.py
```

Upload generated batches to MinIO:

```bash
python3 scripts/upload_incremental_batches_to_minio.py
```

Batch files are uploaded to Hive-style raw partition paths:

```text
hotel_booking_demand/incremental_batches/
  etl_year=2026/
    etl_month=01/
      etl_day=01/
        watermark_date=20260101/
          raw_batch_sequence=001/
            batch_001_initial.csv
```

These partition folders describe ingestion/watermark metadata for raw storage. SCD2 still uses `booking_key`, `batch_id`, `batch_sequence`, and `batch_effective_at`.

Bronze Iceberg `raw_hotel_bookings_history` is partitioned by `watermark_date` for daily batch pruning. Iceberg warehouse folders are managed by Iceberg metadata; use SQL lineage columns instead of manually reading warehouse paths.

Run Spark/Iceberg ingestion inside the Airflow container:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/ingest_batches_to_iceberg.py \
  --batch-dir /opt/airflow/data/input/incremental_batches
```

Build Silver Iceberg tables manually if needed:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/build_silver_iceberg_tables.py
```

## Run dbt Manually if Needed

Run inside Airflow container:

```bash
docker compose exec airflow-webserver dbt debug \
  --project-dir /opt/airflow/dbt/hotel_booking \
  --profiles-dir /opt/airflow/dbt/hotel_booking
```

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

## Validate StarRocks Tables

Check database and tables:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM hotel_booking;"
```

Check StarRocks external catalog:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW CATALOGS;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW DATABASES FROM iceberg_catalog;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM iceberg_catalog.hotel_booking_lakehouse;"
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW TABLES FROM iceberg_catalog.hotel_booking_silver;"
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

Check raw partition metadata in Bronze Iceberg:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT batch_id, etl_year, etl_month, etl_day, watermark_date, raw_batch_sequence, COUNT(*) AS row_count
FROM hotel_booking.stg_iceberg_raw_hotel_bookings
GROUP BY batch_id, etl_year, etl_month, etl_day, watermark_date, raw_batch_sequence
ORDER BY raw_batch_sequence;
"
```

Check Silver Iceberg row counts:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT 'deduped_hotel_bookings' AS table_name, COUNT(*) AS row_count
FROM iceberg_catalog.hotel_booking_silver.deduped_hotel_bookings
UNION ALL
SELECT 'hotel_booking_versions', COUNT(*)
FROM iceberg_catalog.hotel_booking_silver.hotel_booking_versions
UNION ALL
SELECT 'current_hotel_bookings', COUNT(*)
FROM iceberg_catalog.hotel_booking_silver.current_hotel_bookings
UNION ALL
SELECT 'booking_metrics', COUNT(*)
FROM iceberg_catalog.hotel_booking_silver.booking_metrics;
"
```

Check SCD2 fixtures:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, COUNT(*) AS version_count
FROM hotel_booking.int_hotel_booking_versions
WHERE booking_key IN ('hotel_booking_demand:1', 'hotel_booking_demand:2')
GROUP BY booking_key;
"
```

Check SCD2 quality validations:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT booking_key, batch_id, COUNT(DISTINCT record_hash) AS hash_count
FROM hotel_booking.stg_iceberg_raw_hotel_bookings
GROUP BY booking_key, batch_id
HAVING COUNT(DISTINCT record_hash) > 1;
"
```

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT a.booking_key
FROM hotel_booking.int_hotel_booking_versions a
JOIN hotel_booking.int_hotel_booking_versions b
  ON a.booking_key = b.booking_key
 AND a.valid_from < COALESCE(b.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND b.valid_from < COALESCE(a.valid_to, CAST('9999-12-31 00:00:00' AS DATETIME))
 AND a.valid_from <> b.valid_from
LIMIT 10;
"
```

Check key marts:

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

## StarRocks Materialized Views

Materialized Views run in the main Airflow DAG after `dbt_test`. They demonstrate StarRocks precomputed aggregation and query rewrite behavior after dbt has created `fact_bookings`.

Current MVs:

- `mv_daily_booking_revenue`
- `mv_monthly_booking_revenue`
- `mv_hotel_performance`

The DAG runs this script automatically. Run it manually only when testing this layer directly:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/apply_starrocks_materialized_views.py
```

Manual StarRocks checks:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW MATERIALIZED VIEWS FROM hotel_booking;"
```

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT 'mv_daily_booking_revenue' AS object_name, COUNT(*) AS row_count
FROM hotel_booking.mv_daily_booking_revenue
UNION ALL
SELECT 'mart_daily_booking_revenue', COUNT(*)
FROM hotel_booking.mart_daily_booking_revenue
UNION ALL
SELECT 'mv_monthly_booking_revenue', COUNT(*)
FROM hotel_booking.mv_monthly_booking_revenue
UNION ALL
SELECT 'mart_monthly_booking_revenue', COUNT(*)
FROM hotel_booking.mart_monthly_booking_revenue
UNION ALL
SELECT 'mv_hotel_performance', COUNT(*)
FROM hotel_booking.mv_hotel_performance
UNION ALL
SELECT 'mart_hotel_performance', COUNT(*)
FROM hotel_booking.mart_hotel_performance;
"
```

The DAG also validates query rewrite with `EXPLAIN`: a matching aggregate query written against `fact_bookings` should scan `mv_daily_booking_revenue`.

Important: Superset dashboard still uses dbt mart tables by default. The `mv_*` objects are the StarRocks optimization layer, not the business source of truth.

Full checklist: [docs/final_validation_checklist.md](docs/final_validation_checklist.md)

## Superset Dashboard

Superset StarRocks SQLAlchemy URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Create/update the demo dashboard automatically:

```bash
docker compose up -d superset
docker compose exec superset python /app/bootstrap_scripts/bootstrap_superset_dashboard.py
```

The bootstrap script creates:

- database connection `StarRocks Hotel Booking`
- 10 datasets from mart tables
- 13 demo charts
- dashboard `Hotel Booking BI Dashboard`

If the Superset container has not been recreated after the `scripts/` mount was added, use:

```bash
docker compose cp scripts/bootstrap_superset_dashboard.py superset:/tmp/bootstrap_superset_dashboard.py
docker compose exec superset python /tmp/bootstrap_superset_dashboard.py
```

Open dashboard:

```text
http://localhost:8088/superset/dashboard/hotel-booking-bi-dashboard/
```

Superset datasets must come from mart tables only:

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

Dashboard name:

```text
Hotel Booking BI Dashboard
```

Dashboard docs:

- [docs/dashboard_plan.md](docs/dashboard_plan.md)
- [docs/superset_dashboard_guide.md](docs/superset_dashboard_guide.md)

Dashboard charts should query mart datasets only, not raw/staging/intermediate tables.

## Known Limitations

- Dataset is not real Vinpearl data.
- Kaggle dataset has no natural booking ID, so this MVP uses synthetic `booking_key = source_dataset + original_source_row_number`.
- Dataset has no real cost/expense fields.
- `estimated_revenue = adr * total_nights`.
- `realized_revenue = estimated_revenue` only for non-cancelled bookings.
- Any cost/margin/PNL is simulated only and should be labelled clearly.
- No realtime/streaming in this MVP.
- No Cube.dev, semantic layer, or Agentic AI in this MVP.
- Local Docker performance is not a production benchmark.
- Iceberg external tables are managed by Iceberg, not by StarRocks table types. StarRocks table type choices apply only to internal dbt materializations.
- Silver Iceberg tables are rebuilt deterministically for the MVP. Production can switch this step to incremental/MERGE logic if needed.
- StarRocks Materialized Views are optimization objects refreshed by the Airflow DAG; dbt mart tables remain the business source of truth.

## StarRocks Table Types

| Layer / model | Storage owner | StarRocks table type |
| --- | --- | --- |
| Bronze `iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history` | Iceberg external table | Not applicable |
| Silver `iceberg_catalog.hotel_booking_silver.deduped_hotel_bookings` | Iceberg external table | Not applicable |
| Silver `iceberg_catalog.hotel_booking_silver.hotel_booking_versions` | Iceberg external table for SCD2/version history | Not applicable |
| Silver `iceberg_catalog.hotel_booking_silver.current_hotel_bookings` | Iceberg external table | Not applicable |
| Silver `iceberg_catalog.hotel_booking_silver.booking_metrics` | Iceberg external table | Not applicable |
| `stg_iceberg_raw_hotel_bookings` | StarRocks view over Bronze Iceberg raw history | View, no table type |
| `int_hotel_bookings_deduped` | StarRocks view over Silver Iceberg table | View, no StarRocks table type |
| `int_hotel_booking_versions` | StarRocks view over Silver SCD2/version table | View, no StarRocks table type |
| `int_current_hotel_bookings` current model | StarRocks view over Silver current table | View, no StarRocks table type |
| `int_booking_metrics` | StarRocks view over Silver metrics table | View, no StarRocks table type |
| `fact_bookings` | StarRocks internal Gold dbt table | `PRIMARY KEY(booking_key)` |
| `dim_*` tables | StarRocks internal Gold dbt tables | `PRIMARY KEY` where configured |
| `mart_*` tables | StarRocks internal Gold dbt tables | `DUPLICATE KEY` for MVP |
| `mv_*` materialized views | StarRocks internal MV layer | `REFRESH MANUAL` aggregate acceleration/query rewrite |

## Troubleshooting

Container OOM or restart:

- Increase Docker Desktop memory.
- Stop services not needed for the current test.
- Keep dbt `threads` low.

Airflow cannot see scripts:

```bash
docker compose up -d --build airflow-init airflow-webserver airflow-scheduler
```

Iceberg external catalog fails:

- Confirm `iceberg-rest` is running.
- Confirm MinIO has bucket `warehouse`.
- Confirm StarRocks version supports Iceberg external catalog.
- Recreate the catalog only after Iceberg REST and StarRocks are healthy.

Spark/Iceberg ingestion fails with AWS region or credentials errors:

- Confirm `.env` includes `AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.
- For local MinIO, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` should match `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.
- Rebuild/restart Airflow after changing Docker image dependencies:
  `docker compose up -d --build airflow-init airflow-webserver airflow-scheduler`.

StarRocks not reachable:

```bash
docker compose ps starrocks
docker compose logs -f starrocks
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT 1;"
```

Superset connection fails:

- Use host `starrocks`, not `localhost`, inside Superset.
- Use URI `starrocks://root:@starrocks:9030/default_catalog.hotel_booking`.
- Confirm StarRocks and Superset are on `data_network`.

Dashboard disappeared:

- `docker compose down` keeps local volumes.
- `docker compose down -v` deletes volumes, including all Superset metadata.
