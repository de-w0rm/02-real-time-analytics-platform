# Backfill Strategy

## 1. Purpose

Backfill allows reprocessing historical data for:

- Schema evolution
- Bug fixes
- New aggregations
- Corrected business logic

The design assumes Kafka retention is configured to allow replay.

---

## 2. Replay from Kafka

Approach:

1. Stop the streaming job.
2. Delete or redirect the checkpoint directory.
3. Restart Spark with:
   - `startingOffsets = earliest`
   - New checkpoint location (isolated run)

This ensures a clean reprocessing cycle.

---

## 3. Controlled Backfill

For partial replay:

- Use Kafka offsets or timestamps.
- Configure Spark to start from specific offsets.
- Write to a separate output path.
- Validate results before promoting.

---

## 4. Idempotency Considerations

Backfill must:

- Maintain deduplication via `event_id`
- Avoid double counting in Gold
- Write to isolated paths before merge

---

## 5. Production Considerations

In real production systems:

- Backfill runs in isolated compute environments
- Output is validated before promotion
- Downstream systems are version-aware
- Metadata catalogs track dataset versions

---

## 6. Risk Mitigation

- Never overwrite production partitions directly
- Always validate record counts and aggregates
- Maintain reproducible runs
- Document reprocessing parameters