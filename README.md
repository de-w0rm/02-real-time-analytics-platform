# 02 - Real-Time Analytics Platform

A production-oriented real-time analytics platform built entirely with open-source tools and runnable locally via Docker.

This project simulates a SaaS event-driven architecture using:

- Apache Kafka (event backbone)
- Schema Registry (Avro data contracts)
- Spark Structured Streaming (stateful stream processing)
- Parquet (analytical storage layer)
- DuckDB (local analytical queries)

The goal is to model real production streaming patterns while remaining fully self-contained and reproducible.

---

## What This Project Demonstrates

This repository showcases senior-level engineering concepts including:

- Event-driven architecture
- Streaming data pipelines
- Event-time windowed aggregations
- Watermarking and late-data handling
- Idempotency and deduplication
- Schema evolution with Avro
- Stateful stream processing
- Replay-safe design
- Deterministic data quality validation
- Failure recovery with checkpointing

This is structured as a production-style system, not a tutorial demo.

---

## High-Level Architecture

Event Generator  
→ Kafka (`sales.events.v1`)  
→ Spark Structured Streaming  
→ Parquet (Gold Layer)  
→ DuckDB (Analytical Queries)

Key characteristics:

- Immutable event log
- Contract-enforced schema (Avro + Schema Registry)
- Event-time processing with watermarking
- Deduplication by idempotency key (`event_id`)
- Stateful 5-minute revenue windows
- Checkpointed streaming jobs
- Idempotent Data Quality layer

---

## Data Model

Events represent SaaS sales activity and include:

- `event_id` (idempotency key)
- `event_time` (event-time processing anchor)
- `ingest_time` (latency measurement)
- `account_id` (partition key)
- `plan_tier`, `region`, `currency`
- `amount`

Events are serialized in Avro and validated via Schema Registry.

Schema evolution follows backward-compatible principles.

---

## Data Quality Layer

Data quality validation runs per micro-batch using `foreachBatch`.

Validation rules include:

- Required field checks
- Domain validation (`plan_tier`, `region`, `currency`)
- Numeric validation (`amount`)
- SLA latency validation

Design characteristics:

- Single-pass aggregation (no multiple Spark actions)
- Idempotent writes per `batch_id`
- Deterministic overwrite semantics
- Optional capped violation samples
- Replay-safe outputs

Output structure:

```
/data/dq/revenue_5m/
    metrics/batch_id=<id>/
    samples/batch_id=<id>/
```

---

## Reliability Model

Kafka: At-least-once delivery  
Spark: Effectively-once (via deduplication + checkpointing)

Mechanisms:

- Kafka offset tracking
- Spark checkpointing for state recovery
- Watermark to bound state growth
- Deduplication on `event_id`
- Idempotent DQ output per batch

The system tolerates:

- Spark restarts
- Kafka restarts
- Producer restarts

without data corruption or revenue double-counting.

---

## How to Run

Start the platform:

```bash
docker compose up --build
```

This launches:

- Kafka
- Schema Registry
- Spark (master + worker)
- Event producer
- Streaming processor

Events begin flowing automatically once services are up.

---

## Querying Results

Gold aggregation output is written to:

```
/data/gold/revenue_5m
```

You can query results using DuckDB:

```bash
duckdb
```

Example query:

```sql
SELECT *
FROM 'data/gold/revenue_5m/*.parquet'
ORDER BY window_start DESC
LIMIT 20;
```

---

## Repository Structure

```
docs/               → Architecture & design documentation
infra/              → Docker configuration
producer/           → Event generator
spark_jobs/         → Spark streaming logic
data/               → Runtime volumes (gitignored)
```

---

## Failure Scenarios Simulated

The platform intentionally simulates:

- Duplicate events
- Late-arriving data
- Schema evolution
- Spark job restart recovery
- Kafka restart recovery

These scenarios validate watermarking, state recovery, and idempotent processing.

---

## Architectural Trade-offs

To keep the system runnable locally:

- Bronze raw persistence is omitted
- Local filesystem is used instead of distributed storage
- Limited Kafka partitions (3) for laptop constraints
- DLQ topic is architecturally defined but not actively written to

These constraints simplify local execution without compromising architectural integrity.

---

## Why This Project Matters

This project models real production concerns such as:

- At-least-once vs effectively-once semantics
- Stateful streaming and memory boundaries
- Watermark-driven state cleanup
- Deterministic idempotent outputs
- Replay and recovery safety
- Scalability boundaries and partition strategy

It demonstrates how modern SaaS analytics pipelines are designed, not just how they run.