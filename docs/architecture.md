# Architecture

Tài liệu này mô tả architecture của local BI POC. Mục tiêu là giữ tinh thần của production-like flow, nhưng triển khai đơn giản hơn để chạy được trên laptop.

## Original Idea

Architecture gốc trong dự án production-like có thể dùng:

```text
Source / VPL System
  -> Ingestion Layer
  -> Raw S3
  -> Redshift transformation
  -> Curated CDP / finance mart / pre-aggregation
  -> Redshift / ClickHouse serving
  -> Cube.dev semantic layer
  -> BI Portal / Dashboard / ML / Agentic BI
```

POC này không rebuild full architecture đó.

## Local POC Architecture

Architecture local MVP:

```text
CSV
  -> MinIO raw
  -> Spark append-only Iceberg raw history
  -> StarRocks external catalog
  -> dbt dedup/SCD2/current/fact/dim/marts
  -> StarRocks Materialized Views for selected aggregations
  -> Superset dashboard
```

Mermaid diagram:

```mermaid
flowchart LR
    A[Hotel Booking CSV] --> B[Generate synthetic batches]
    B --> C[MinIO raw CSV bucket]
    C --> D[Spark technical pre-transform]
    D --> E[Iceberg raw history on MinIO warehouse]
    E --> F[StarRocks external catalog]
    F --> G[dbt dedup and SCD2]
    G --> H[StarRocks current/fact/dim/mart tables]
    H --> K[StarRocks Materialized Views]
    H --> I[Superset datasets]
    K -. query rewrite optimization .-> I
    I --> J[Hotel Booking BI Dashboard]
```

## Scope

This MVP is batch-only.

Included:

- Local CSV ingestion.
- Raw object storage in MinIO.
- Append-only Iceberg raw history.
- StarRocks external catalog for Iceberg.
- SCD2/current/fact/dim/mart tables in StarRocks.
- dbt transformations inside StarRocks.
- StarRocks Materialized Views for selected aggregate/query rewrite validation.
- Airflow manual DAG orchestration.
- Superset dashboard from mart tables.

Not included:

- Realtime/streaming.
- Cube.dev.
- Semantic layer.
- Agentic AI.
- Formal performance benchmark.
- Real PNL, real cost, or real profit calculation.

## Layer Responsibilities

| Layer | Tool | Responsibility |
| --- | --- | --- |
| Source CSV | `data/input/hotel_bookings.csv` | Local Kaggle dataset used as hospitality BI sample data. |
| Synthetic batches | Python | Generates deterministic incremental batch CSVs with persisted `booking_key`. |
| Raw storage | MinIO | Stores immutable raw batch CSVs under `hotel-booking-raw/hotel_booking_demand/incremental_batches/`. |
| Technical pre-transform | Spark / PySpark | Enforces schema, enriches metadata, computes business-only `record_hash`, and appends to Iceberg. |
| Lakehouse history | Iceberg | Stores append-only raw history. Physical data format is Parquet managed by Iceberg. |
| External catalog | StarRocks | Queries Iceberg tables through `iceberg_catalog`; StarRocks does not own these external table types. |
| Transformation | dbt | Dedup, SCD2, current-state, fact, dimension, and mart models inside StarRocks. |
| Warehouse / serving | StarRocks | Stores internal dbt tables and serves Superset queries from mart tables. |
| Optimization | StarRocks Materialized View | Precomputes selected aggregations and validates query rewrite after dbt tests pass. |
| Orchestration | Airflow | Runs the batch pipeline manually: profile, generate, upload, Spark ingest, dbt run/test, MV optimization, mart validation. |
| Dashboard | Superset | Creates datasets and charts from StarRocks mart tables. |

## Important dbt Note

dbt does not transform files directly in MinIO.

Actual flow:

```text
CSV -> MinIO raw batch CSVs -> Iceberg raw history -> StarRocks external catalog -> dbt models in StarRocks -> StarRocks mart tables
```

MinIO is the raw object storage layer. Iceberg stores historical raw batches. StarRocks reads Iceberg through external catalog and stores the dbt-created internal serving tables.

## SCD2, Dedup, And Incremental Logic

- `booking_key = source_dataset + original_source_row_number` is generated once by the batch generator and persisted in every generated batch.
- Spark computes `record_hash` from normalized business columns only. Ingestion metadata and derived metrics are excluded from the hash.
- dbt first validates that each `booking_key + batch_id` has at most one business state.
- dbt exact dedup collapses duplicate rows by `booking_key + batch_id + record_hash`.
- dbt SCD2 orders records by `booking_key`, `batch_sequence`, and `batch_effective_at`.
- SCD2 is built from change records only. Consecutive same `record_hash` values are compressed into one version.
- `valid_from = batch_effective_at`; `valid_to` is calculated with `LEAD(valid_from)` over change records.
- The current model keeps only the latest `is_current = 1` version per `booking_key`.

## StarRocks Table Type Note

| Layer / model | Storage owner | StarRocks table type |
| --- | --- | --- |
| `iceberg_catalog.hotel_booking_lakehouse.raw_hotel_bookings_history` | Iceberg external table | Not applicable |
| `stg_iceberg_raw_hotel_bookings` | StarRocks view over external Iceberg table | View, no table type |
| `int_hotel_bookings_deduped` | StarRocks internal dbt table | `DUPLICATE KEY(booking_key, batch_id)` |
| `scd_hotel_bookings` | StarRocks internal dbt table | `DUPLICATE KEY(booking_key, valid_from)` |
| `int_current_hotel_bookings` current model | StarRocks internal dbt table | `PRIMARY KEY(booking_key)` |
| `int_booking_metrics` | StarRocks internal dbt table | `PRIMARY KEY(booking_key)` |
| `fact_bookings` | StarRocks internal dbt table | `PRIMARY KEY(booking_key)` |
| `mart_*` tables | StarRocks internal dbt tables | `DUPLICATE KEY` for MVP |
| `mv_*` materialized views | StarRocks internal MV layer | `REFRESH MANUAL` aggregate acceleration/query rewrite |

## Materialized Views

Materialized Views are part of the main Airflow pipeline optimization step. They are created and refreshed after `dbt_test` succeeds.

Current MVs:

- `mv_daily_booking_revenue`
- `mv_monthly_booking_revenue`
- `mv_hotel_performance`

They are created from `hotel_booking.fact_bookings`, refreshed with `REFRESH MANUAL ... WITH SYNC MODE`, and validated against the equivalent dbt mart totals. The DAG also validates query rewrite by checking that a matching aggregate query on `fact_bookings` scans `mv_daily_booking_revenue` in the `EXPLAIN` plan.

Superset still defaults to dbt mart datasets. The MV layer demonstrates StarRocks serving optimization; dbt mart tables remain the business source of truth.

## StarRocks Role in This POC

In the original production-like flow, Redshift and ClickHouse may split warehouse/transformation and serving responsibilities.

In this local MVP, StarRocks replaces both roles:

- warehouse table storage
- transformation target for dbt
- serving source for Superset dashboard

This is a functional local validation, not a formal benchmark.
