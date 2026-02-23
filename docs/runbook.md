# Runbook

## 1. Starting the Platform

Build and start all services:

```bash
docker compose up --build
```

Verify services:

- Kafka UI: http://localhost:<port>
- Schema Registry: http://localhost:<port>
- Spark UI: http://localhost:<port>

---

## 2. Verifying Event Flow

1. Confirm `sales.events.v1` topic exists.
2. Verify messages are being produced.
3. Confirm Spark consumer group is active.
4. Check Parquet files appear under `/data/gold`.

---

## 3. Monitoring

### Kafka

- Check consumer lag.
- Inspect partition distribution.
- Confirm DLQ topic remains low.

### Spark

- Inspect streaming query status.
- Monitor batch duration.
- Validate checkpoint directory updates.

---

## 4. Failure Scenarios

### Duplicate Events

Expected behavior:
- Deduplicated using `event_id`.
- No double counting in Gold layer.

### Late Data

Expected behavior:
- Processed if within watermark.
- Dropped or quarantined if beyond threshold.

### Poison Pill Message

Expected behavior:
- Routed to `sales.events.dlq.v1`.
- Does not crash streaming job.

### Spark Restart

Expected behavior:
- Streaming resumes from last checkpoint.
- No data loss or duplication.

---

## 5. Stopping the Platform

```bash
docker compose down
```

To remove volumes:

```bash
docker compose down -v
```