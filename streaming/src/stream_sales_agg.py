import os
import json
from io import BytesIO
from typing import Dict, Any

import requests
from fastavro import schemaless_reader

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, sum as fsum, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import udf


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales.events.v1")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/data/checkpoints/revenue_5m")
GOLD_DIR = os.getenv("GOLD_DIR", "/data/gold/revenue_5m")

WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")  # must cover your late-event simulation
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

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


def main():
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

    query = (
        agg.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GOLD_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()