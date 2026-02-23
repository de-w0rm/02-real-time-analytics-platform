# 02 - Real-Time Analytics Platform

A production-oriented real-time analytics platform built entirely with open-source tools and runnable locally via Docker.

This project simulates a SaaS event-driven architecture using:

- Apache Kafka (event backbone)
- Spark Structured Streaming (stateful stream processing)
- Schema Registry (data contracts & schema evolution)
- Parquet (analytical storage layer)
- DuckDB (local analytical serving)
- Great Expectations (data quality validation)

---

## Project Goals

This repository demonstrates senior-level knowledge in:

- Event-driven architecture
- Streaming data pipelines
- Windowed aggregations (event-time processing)
- Idempotency & deduplication
- Data quality in streaming systems
- Schema evolution handling
- Failure simulation & recovery
- Backfill and replay strategies
- Production-style documentation (ADRs + runbooks)

---

## High-Level Architecture

Event Generator → Kafka → Spark Streaming → Parquet (Gold Layer) → DuckDB

Key architectural characteristics:

- Immutable event log
- Event-time windowing with watermarking
- Stateful aggregations
- Checkpointed streaming jobs
- Dead-letter queue for bad records
- Streaming-compatible data quality checks

---

## How to Run (Planned)

```bash
docker compose up --build
```

### Planned Services

- Kafka
- Schema Registry
- Spark (master + worker)
- Event Producer
- Streaming Processor
- DuckDB (CLI)
- Kafka UI

---

## Repository Structure

```
docs/               → ADRs, architecture notes, runbooks
infra/              → Docker and infrastructure configs
producer/           → Event generator
streaming/          → Spark processing logic
quality/            → Data quality checks
analytics/          → DuckDB queries
data/               → Runtime volumes (gitignored)
```

---

## Failure Scenarios Simulated

- Duplicate events
- Late-arriving data
- Poison pill messages
- Schema evolution
- Spark job restart recovery
- Kafka restart recovery

---

## Architectural Decisions

See `docs/adr/` for documented decisions and trade-offs.

---

## Why This Project Matters

This project models real production concerns such as:

- Exactly-once vs at-least-once semantics
- Stateful streaming and watermark management
- Idempotent processing
- Backfill strategies
- Scalability boundaries
- Observability & data reliability