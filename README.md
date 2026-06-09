# StarRocks Dataflow

Local BI POC cho hotel booking analytics. Project mô phỏng một flow BI đơn giản theo tinh thần architecture production, nhưng chạy được trên laptop.

## Mục đích POC

Production-like flow trong dự án gốc có thể dùng Redshift / ClickHouse cho warehouse và serving. Trong POC này, StarRocks đóng vai trò cả warehouse và serving layer để validate local MVP:

```text
CSV -> MinIO raw -> StarRocks raw -> dbt marts -> Superset dashboard
```

Đây không phải performance benchmark chính thức. MVP hiện tại không bao gồm realtime/streaming, Cube.dev, semantic layer, hoặc Agentic AI.

## MVP Scope

Included:

- Upload local CSV vào MinIO raw bucket.
- Load CSV từ MinIO/local CSV vào StarRocks raw table.
- Transform bằng dbt inside StarRocks.
- Build fact, dimension, and mart tables trong StarRocks.
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
| StarRocks | Warehouse + serving layer |
| dbt | SQL transformation and mart creation |
| Airflow | Batch orchestration |
| Superset | BI dashboard |

## Architecture

```text
data/input/hotel_bookings.csv
  -> MinIO bucket hotel-booking-raw
  -> StarRocks hotel_booking.raw_hotel_bookings
  -> dbt staging/intermediate/fact/dim/mart tables
  -> Superset datasets and dashboard
```

Important: dbt transforms tables inside StarRocks. dbt does not transform files directly in MinIO.

More detail: [docs/architecture.md](docs/architecture.md)

## Local Resource Notes

StarRocks + Airflow + Superset can be memory-heavy.

Recommended:

- 16GB RAM preferred.
- 8GB RAM possible with conservative Docker limits.
- Increase Docker Desktop memory if containers restart due to OOM.
- Stop optional services when testing one layer only.
- Keep dbt threads low, for example `1` or `2`.

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

Generated summary:

```text
docs/data_profile_summary.md
```

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
  -> validation
```

The DAG performs:

- check CSV exists
- profile dataset
- upload CSV to MinIO
- wait for StarRocks
- create StarRocks database/raw table
- load raw data
- validate raw row count
- run dbt debug/run/test
- log mart row counts

Repeated runs use deterministic reload: raw table is truncated before reload to avoid duplicated rows.

## Manual Raw Load if Needed

Install script dependencies:

```bash
python3 -m pip install -r scripts/requirements.txt
```

Run raw load:

```bash
python3 scripts/load_raw_to_starrocks.py --method files
```

Fallback Stream Load:

```bash
python3 scripts/load_raw_to_starrocks.py --method stream
```

The script still uploads the CSV to MinIO, preserving the raw storage step.

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

Check raw row count:

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SELECT COUNT(*) FROM hotel_booking.raw_hotel_bookings;"
```

Expected for current dataset:

```text
119390
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

Full checklist: [docs/final_validation_checklist.md](docs/final_validation_checklist.md)

## Superset Dashboard

Superset StarRocks SQLAlchemy URI:

```text
starrocks://root:@starrocks:9030/default_catalog.hotel_booking
```

Create Superset datasets from mart tables only:

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
- Dataset has no real cost/expense fields.
- `estimated_revenue = adr * total_nights`.
- `realized_revenue = estimated_revenue` only for non-cancelled bookings.
- Any cost/margin/PNL is simulated only and should be labelled clearly.
- No realtime/streaming in this MVP.
- No Cube.dev, semantic layer, or Agentic AI in this MVP.
- Local Docker performance is not a production benchmark.

## Troubleshooting

Container OOM or restart:

- Increase Docker Desktop memory.
- Stop services not needed for the current test.
- Keep dbt `threads` low.

Airflow cannot see scripts:

```bash
docker compose up -d --build airflow-init airflow-webserver airflow-scheduler
```

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

- `docker compose down` keeps volumes.
- `docker compose down -v` deletes volumes, including all Superset metadata.
