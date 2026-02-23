# Architecture Overview

## 1. Purpose

This document describes the architectural design of the Real-Time Analytics Platform, including event flow, processing layers, scalability considerations, and reliability guarantees.

The system is designed to simulate a production-grade SaaS event-driven analytics pipeline using open-source tools.

---

## 2. High-Level Flow

Event Generator  
→ Kafka (`sales.events.v1`)  
→ Spark Structured Streaming  
→ Parquet (Gold Layer)  
→ DuckDB (Analytical Queries)

---

## 3. Event Backbone (Kafka)

### Topics

- `sales.events.v1` — canonical domain events
- `sales.events.dlq.v1` — invalid or non-conforming messages
- `sales.agg.revenue_5m.v1` (optional) — aggregated results
- `dq.results.v1` (optional) — data quality results

### Partition Strategy

Partition Key: `account_id`  
Partitions: 3 (locally, to demonstrate parallelism)

Rationale:
- Preserves ordering per customer
- Enables horizontal scaling
- Avoids hot partitioning based on event type

---

## 4. Data Contract

The `sales_event` is defined using Avro and enforced via Schema Registry.

Key attributes:

- `event_id` → idempotency key
- `event_time` → event-time windowing
- `ingest_time` → latency measurement
- `schema_version` → evolution tracking

Schema evolution strategy:
- Backward-compatible changes only
- New fields must be optional with defaults
- Versioned topic naming for breaking changes

---

## 5. Processing Layers

### Bronze

- Read raw events from Kafka
- Schema validation
- DLQ routing
- Metadata enrichment

### Silver

- Deduplication (`event_id`)
- Watermarking for late data
- Standardization and normalization

### Gold

- 5-minute rolling revenue windows
- Aggregation by `plan_tier`, `region`
- Output as partitioned Parquet

---

## 6. Reliability Model

Kafka Delivery: At-least-once  
Spark Processing: Effectively-once (via dedup + checkpointing)

Mechanisms:

- Checkpoint directory for state recovery
- Watermark to bound state growth
- DLQ for poison messages
- Idempotent aggregation logic

---

## 7. Scalability Considerations

Scalability is achieved through:

- Kafka partitions
- Spark parallelism
- Bounded state via watermark
- Configurable trigger intervals

Future production extensions:

- Multiple Spark workers
- Horizontal scaling via Kubernetes
- State store optimization
- Compaction and file size tuning