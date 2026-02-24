import os
import json
from io import BytesIO
from typing import Dict, Any

import requests
from fastavro import schemaless_reader

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as fsum, expr, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales.events.v1")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/checkpoints/revenue_5m")
GOLD_DIR = os.getenv("GOLD_DIR", "/data/gold/revenue_5m")

WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")  # must cover your late-event simulation
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

DQ_DIR = os.getenv("DQ_DIR", "/data/dq/revenue_5m")
LATENCY_SLA_SECONDS = int(os.getenv("LATENCY_SLA_SECONDS", "300"))  # 5 minutes


# Cache schemas by id to avoid hitting Schema Registry every row
_schema_cache: Dict[int, Dict[str, Any]] = {}


def _get_avro_schema(schema_id: int) -> Dict[str, Any]:
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    url = f"{SCHEMA_REGISTRY}/schemas/ids/{schema_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    schema_str = r.json()["schema"]
    schema = json.loads(schema_str)
    _schema_cache[schema_id] = schema
    return schema


def _decode_confluent_avro(value: bytes) -> str:
    """
    Confluent wire format:
      byte 0: magic = 0
      bytes 1-4: schema id (big endian int32)
      bytes 5..: avro payload
    Returns JSON string for Spark from_json().
    """
    if value is None:
        return None

    if len(value) < 5 or value[0] != 0:
        # Not a confluent-avro message; return a minimal DLQ-like JSON
        return json.dumps({"_decode_error": "invalid_confluent_wire_format"})

    schema_id = int.from_bytes(value[1:5], byteorder="big", signed=False)
    payload = value[5:]

    schema = _get_avro_schema(schema_id)
    record = schemaless_reader(BytesIO(payload), schema)
    return json.dumps(record)


decode_udf = udf(_decode_confluent_avro, StringType())


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("ingest_time", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("plan_tier", StringType(), False),
    StructField("region", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("source", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("schema_version", IntegerType(), False),
])


def write_dq_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """
    Lightweight DQ metrics per micro-batch.

    Goals:
      - Single-pass aggregation (avoid multiple count() actions)
      - Idempotent output per batch_id (safe on retries/restarts)
      - Optional capped sample of violating rows for debugging

    Output layout under DQ_DIR (default: /data/dq/revenue_5m):
      - {DQ_DIR}/metrics/batch_id=<id>/
      - {DQ_DIR}/samples/batch_id=<id>/
    """
    if batch_df.rdd.isEmpty():
        return

    required_cols = ["event_id", "event_ts", "amount", "region", "plan_tier", "currency", "ingest_ts"]

    valid_plan = ["free", "pro", "enterprise"]
    valid_region = ["NA", "EU", "LATAM", "APAC"]
    valid_currency = ["USD", "CAD", "EUR"]

    # Rule flags (pure column expressions)
    null_required_flag = F.lit(False)
    for c in required_cols:
        null_required_flag = null_required_flag | F.col(c).isNull()

    invalid_domain_flag = (
        (~F.col("plan_tier").isin(valid_plan))
        | (~F.col("region").isin(valid_region))
        | (~F.col("currency").isin(valid_currency))
    )

    bad_amount_flag = (
        F.col("amount").isNull()
        | (F.col("amount") == F.lit(0))
        | F.isnan("amount")
    )

    latency_breach_flag = (
        (F.col("ingest_ts").cast("long") - F.col("event_ts").cast("long"))
        > F.lit(LATENCY_SLA_SECONDS)
    )

    # IMPORTANT: compute "invalid_any" as a single boolean to avoid double-count subtraction
    invalid_any_flag = null_required_flag | invalid_domain_flag | bad_amount_flag

    # Single-pass aggregation
    metrics_row = (
        batch_df.select(
            null_required_flag.alias("f_null_required"),
            invalid_domain_flag.alias("f_invalid_domain"),
            bad_amount_flag.alias("f_bad_amount"),
            latency_breach_flag.alias("f_latency_breach"),
            invalid_any_flag.alias("f_invalid_any"),
        )
        .agg(
            F.count(F.lit(1)).alias("total_records"),
            F.sum(F.col("f_null_required").cast("int")).alias("null_required_fields"),
            F.sum(F.col("f_invalid_domain").cast("int")).alias("invalid_domain_values"),
            F.sum(F.col("f_bad_amount").cast("int")).alias("bad_amounts"),
            F.sum(F.col("f_latency_breach").cast("int")).alias("latency_sla_breaches"),
            F.sum(F.col("f_invalid_any").cast("int")).alias("invalid_any"),
        )
        .withColumn("batch_id", F.lit(int(batch_id)))
        .withColumn("dq_run_ts", F.current_timestamp())
        .withColumn("valid_records_estimate", F.expr("greatest(total_records - invalid_any, 0)"))
        .select(
            "dq_run_ts",
            "batch_id",
            "total_records",
            "valid_records_estimate",
            "null_required_fields",
            "invalid_domain_values",
            "bad_amounts",
            "latency_sla_breaches",
        )
    )

    # Idempotent write: deterministic per-batch path + overwrite
    metrics_out = f"{DQ_DIR}/metrics/batch_id={int(batch_id)}"
    (
        metrics_row.coalesce(1)  # tiny output: keep it as one file
        .write.mode("overwrite")
        .json(metrics_out)
    )

    # Optional: sample violating rows (capped) for debugging
    sample_limit = 100
    samples_out = f"{DQ_DIR}/samples/batch_id={int(batch_id)}"

    violations = (
        batch_df.withColumn(
            "dq_violation_reason",
            F.concat_ws(
                "|",
                F.when(null_required_flag, F.lit("NULL_REQUIRED")),
                F.when(invalid_domain_flag, F.lit("INVALID_DOMAIN")),
                F.when(bad_amount_flag, F.lit("BAD_AMOUNT")),
                F.when(latency_breach_flag, F.lit("LATENCY_SLA_BREACH")),
            ),
        )
        .filter(null_required_flag | invalid_domain_flag | bad_amount_flag | latency_breach_flag)
        .limit(sample_limit)
    )

    (
        violations.write.mode("overwrite").json(samples_out)
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("sales-revenue-5m")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Decode Confluent Avro -> JSON string -> Struct
    decoded = raw.select(
        col("key").cast("string").alias("kafka_key"),
        decode_udf(col("value")).alias("json_str"),
        col("timestamp").alias("kafka_ingest_ts"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )

    events = (
        decoded
        .withColumn("event", from_json(col("json_str"), EVENT_SCHEMA))
        .select(
            "kafka_key", "kafka_ingest_ts", "kafka_partition", "kafka_offset",
            col("event.*")
        )
        # Parse timestamps
        .withColumn("event_ts", to_timestamp(col("event_time")))
        .withColumn("ingest_ts", to_timestamp(col("ingest_time")))
        # Basic sanity: drop rows where parsing failed
        .filter(col("event_id").isNotNull() & col("event_ts").isNotNull())
    )

    # Dedup + watermark (bounds state)
    deduped = (
        events
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )

    # Data quality metrics stream
    dq_query = (
        deduped.writeStream
        .foreachBatch(write_dq_metrics)
        .outputMode("append")  # foreachBatch ignores output mode, but keeping explicit is fine
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "_dq"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # 5-minute revenue windows (event-time)
    agg = (
        deduped
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("region"),
            col("plan_tier"),
        )
        .agg(
            fsum(col("amount")).alias("revenue"),
            expr("count(1)").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "region",
            "plan_tier",
            "revenue",
            "event_count",
        )
    )

    agg_query = (
        agg.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GOLD_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Wait for either query to terminate (and keep process alive)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()