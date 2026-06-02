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
  -> StarRocks raw table
  -> dbt marts
  -> Superset dashboard
```

Mermaid diagram:

```mermaid
flowchart LR
    A[Hotel Booking CSV] --> B[Airflow upload task]
    B --> C[MinIO raw bucket]
    C --> D[StarRocks raw_hotel_bookings]
    D --> E[dbt staging/intermediate]
    E --> F[StarRocks fact/dim/mart tables]
    F --> G[Superset datasets]
    G --> H[Hotel Booking BI Dashboard]
```

## Scope

This MVP is batch-only.

Included:

- Local CSV ingestion.
- Raw object storage in MinIO.
- Raw table and mart tables in StarRocks.
- dbt transformations inside StarRocks.
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
| Raw storage | MinIO | Stores raw CSV object at `hotel-booking-raw/hotel_booking_demand/hotel_bookings.csv`. |
| Warehouse / serving | StarRocks | Stores raw, staging, fact/dim, and mart tables. Also serves Superset queries. |
| Transformation | dbt | Casts/cleans raw data and builds fact, dimension, and mart tables inside StarRocks. |
| Orchestration | Airflow | Runs the batch pipeline manually: profile, upload, load raw, dbt run/test, mart validation. |
| Dashboard | Superset | Creates datasets and charts from StarRocks mart tables. |

## Important dbt Note

dbt does not transform files directly in MinIO.

Actual flow:

```text
CSV -> MinIO raw object -> StarRocks raw table -> dbt models in StarRocks -> StarRocks mart tables
```

MinIO is the raw storage layer. StarRocks is where SQL transformation and serving happen.

## StarRocks Role in This POC

In the original production-like flow, Redshift and ClickHouse may split warehouse/transformation and serving responsibilities.

In this local MVP, StarRocks replaces both roles:

- warehouse table storage
- transformation target for dbt
- serving source for Superset dashboard

This is a functional local validation, not a formal benchmark.
