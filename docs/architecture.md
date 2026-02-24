# Architecture Overview

## 1. Purpose

This document describes the architecture of the **Real-Time Analytics Platform**, a production-oriented streaming analytics system built entirely with open-source components and runnable locally via Docker.

The objective is to simulate a realistic SaaS analytics pipeline with:

- Event-driven ingestion
- Schema enforcement
- Stateful stream processing
- Event-time windowed aggregations
- Data quality validation
- Recoverability and replay safety

The system reflects production design patterns while remaining lightweight and self-contained.

---

## 2. System Architecture

### End-to-End Flow

Event Generator  
→ Kafka (`sales.events.v1`)  
→ Spark Structured Streaming  
→ Parquet (Gold Layer)  
→ DuckDB (Analytical Queries)

### Core Responsibilities

| Component | Responsibility |
|------------|---------------|
| Event Generator | Simulates SaaS domain events |
| Kafka | Durable event backbone |
| Schema Registry | Enforces Avro data contracts |
| Spark | Stateful stream processing |
| Parquet | Analytical storage layer |
| DuckDB | Local OLAP query engine |

This mirrors a common production pattern: log-based ingestion + stateful processing + columnar analytical storage.

---

## 3. Event Backbone (Kafka)

### Topic

- `sales.events.v1` — canonical domain event stream

DLQ topics are architecturally supported but not actively written to in this local implementation to keep the system lightweight.

### Partition Strategy

Partition Key: `account_id`  
Partitions: 3 (local demonstration)

Rationale:

- Preserves ordering per account
- Enables parallel consumption
- Demonstrates horizontal scaling capability
- Avoids skew based on event type

In production, partition count would scale with expected throughput and consumer parallelism.

---

## 4. Data Contract & Schema Management

Events are serialized using **Avro** and validated via **Schema Registry**.

Key attributes:

- `event_id` — idempotency key
- `event_time` — event-time windowing anchor
- `ingest_time` — end-to-end latency measurement
- `schema_version` — evolution tracking

### Schema Evolution Strategy

- Backward-compatible changes only
- New fields must be optional with defaults
- Breaking changes require new topic version

This aligns with common enterprise event governance standards.

---

## 5. Processing Model (Spark Structured Streaming)

The processing layer implements a simplified medallion-style design.

### Bronze Layer (In-Memory)

- Kafka ingestion
- Avro deserialization
- Schema parsing
- Metadata extraction (partition, offset, timestamp)

Raw persistence is intentionally omitted to reduce local storage complexity.

---

### Silver Layer

- Event-time parsing
- Deduplication (`dropDuplicates(event_id)`)
- Watermark application
- Late-event handling

Watermark configuration:

`WATERMARK_DELAY = 10 minutes`

This bounds state growth and prevents unbounded memory accumulation.

---

### Gold Layer

5-minute tumbling windows over `event_ts`.

Grouped by:
- `region`
- `plan_tier`

Metrics:
- `revenue`
- `event_count`

Output:
- Partitioned Parquet
- Append mode
- Checkpointed

This represents the analytical serving layer.

---

## 6. Data Quality Layer

Data quality validation executes per micro-batch via `foreachBatch`.

Validation Rules:

- Required field presence
- Domain validation (`plan_tier`, `region`, `currency`)
- Numeric validation (`amount`)
- SLA validation (event latency threshold)

### Design Characteristics

- Single-pass aggregation (no multiple Spark actions)
- Idempotent output per batch (`batch_id`)
- Deterministic overwrite semantics
- Optional capped violation samples

Output structure:
/data/dq/revenue_5m/
metrics/batch_id=<id>/
samples/batch_id=<id>/


This ensures replay safety and auditability.

---

## 7. Reliability Model

### Delivery Semantics

Kafka: At-least-once  
Spark: Effectively-once (via deduplication + checkpointing)

### Failure Recovery

- Kafka offsets committed after successful batch completion
- Spark checkpoint stores offsets and state
- Restart resumes from last consistent state
- DQ outputs overwritten per batch to prevent duplication
- Deduplication prevents revenue double counting

The system tolerates:

- Spark restarts
- Kafka restarts
- Producer restarts

without data corruption.

---

## 8. State Management

State is maintained for:

- Deduplication by `event_id`
- 5-minute event-time window aggregations

Watermark ensures:

- Bounded state retention
- Automatic cleanup of expired windows
- Memory stability

State growth is controlled by:

- Watermark delay
- Trigger interval
- Shuffle partition configuration

---

## 9. Scalability Considerations

Scalability is achieved through:

- Kafka partitioning
- Spark parallelism
- Stateless ingestion
- Bounded state

To scale beyond local mode:

- Increase Kafka partitions
- Increase Spark executors
- Deploy on a multi-node cluster
- Externalize storage to distributed filesystem
- Tune Parquet file sizes and compaction strategy

No architectural changes are required to scale horizontally.

---

## 10. Trade-offs

This implementation intentionally:

- Omits raw Bronze persistence
- Uses local filesystem instead of distributed storage
- Uses limited partitions for laptop constraints
- Avoids active DLQ routing to reduce demo complexity

These trade-offs reduce operational overhead while preserving architectural integrity.

---

# Summary

This platform demonstrates:

- Event-driven architecture
- Contract-based schema governance
- Stateful streaming with bounded memory
- Event-time windowed aggregations
- Idempotent recovery
- Structured data quality enforcement
- Production-aligned design decisions

The system is replay-safe, horizontally scalable, and architecturally representative of modern SaaS analytics pipelines.