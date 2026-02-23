# ADR 0001: Event Contract and Topic Design

## Status
Accepted

## Context
We need a production-like event backbone that supports replay, schema evolution, and scalable consumption for streaming analytics.

## Decision
- Define a canonical `sales_event` domain event with an explicit schema (versioned).
- Publish events to Kafka topic `sales.events.v1`.
- Use `account_id` as the partition key.
- Provision 3 partitions for local scalability demonstration.
- Use a Dead Letter Queue topic `sales.events.dlq.v1` for invalid or non-conforming events.
- Adopt Schema Registry and Avro serialization to enforce data contracts and enable controlled schema evolution.

## Consequences
### Positive
- Strong schema contracts prevent silent breaking changes.
- Replay and backfill are enabled via Kafka retention.
- DLQ provides operational safety for poison pill messages.
- Partitioning supports horizontal scaling and reduces consumer bottlenecks.

### Negative / Trade-offs
- Additional operational components (Schema Registry, Avro serializers).
- Requires compatibility management and discipline for schema changes.

## Notes
Schema evolution will be demonstrated by introducing `sales_event_v2` with backward-compatible fields.