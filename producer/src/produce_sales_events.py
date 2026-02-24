import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer


TOPIC = os.getenv("KAFKA_TOPIC", "sales.events.v1")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "sales.events.dlq.v1")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.02"))        # 2%
LATE_EVENT_RATE = float(os.getenv("LATE_EVENT_RATE", "0.05"))      # 5%
MAX_LATE_SECONDS = int(os.getenv("MAX_LATE_SECONDS", "600"))       # up to 10 min late

SEED = os.getenv("SEED")
if SEED:
    random.seed(int(SEED))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_avsc(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_event(now: datetime, event_id: str | None = None) -> Dict[str, Any]:
    event_time = now

    # Late events simulation (event_time behind ingest_time)
    if random.random() < LATE_EVENT_RATE:
        lag = random.randint(5, MAX_LATE_SECONDS)
        event_time = now - timedelta(seconds=lag)

    account_id = f"acct_{random.randint(1, 200):04d}"
    user_id = f"user_{random.randint(1, 5000):05d}" if random.random() < 0.90 else None

    event_type = random.choices(
        ["subscription_created", "renewal", "upgrade", "refund", "chargeback"],
        weights=[0.15, 0.55, 0.15, 0.10, 0.05],
        k=1,
    )[0]

    plan_tier = random.choices(["free", "pro", "enterprise"], weights=[0.20, 0.65, 0.15], k=1)[0]
    region = random.choices(["NA", "EU", "LATAM", "APAC"], weights=[0.45, 0.25, 0.20, 0.10], k=1)[0]
    currency = random.choices(["USD", "CAD", "EUR"], weights=[0.70, 0.20, 0.10], k=1)[0]

    # Amount logic (refund/chargeback negative)
    base = random.choice([29.0, 49.0, 99.0, 199.0, 499.0])
    amount = base * (1.0 if event_type in ["subscription_created", "renewal", "upgrade"] else -1.0)

    source = random.choices(["web", "mobile", "backend", "billing"], weights=[0.45, 0.20, 0.15, 0.20], k=1)[0]

    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_time": iso(event_time),
        "ingest_time": iso(now),
        "account_id": account_id,
        "user_id": user_id,
        "event_type": event_type,
        "plan_tier": plan_tier,
        "region": region,
        "currency": currency,
        "amount": float(amount),
        "source": source,
        "trace_id": str(uuid.uuid4()) if random.random() < 0.80 else None,
        "schema_version": 1,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY_FAILED topic={msg.topic()} key={msg.key()} err={err}")
    else:
        print(f"DELIVERED topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main():
    print("Starting producer with:")
    print(f"  bootstrap.servers = {BOOTSTRAP_SERVERS}")
    print(f"  schema.registry   = {SCHEMA_REGISTRY_URL}")
    print(f"  topic             = {TOPIC}")
    print(f"  events/sec        = {EVENTS_PER_SEC}")
    print(f"  duplicate_rate    = {DUPLICATE_RATE}")
    print(f"  late_event_rate   = {LATE_EVENT_RATE}")
    print(f"  max_late_seconds  = {MAX_LATE_SECONDS}")

    schema_str = load_avsc("schemas/sales_event_v1.avsc")

    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Register schema under a stable subject name (topic-value is common)
    subject = f"{TOPIC}-value"
    schema = Schema(schema_str, "AVRO")
    schema_id = schema_registry.register_schema(subject, schema)
    print(f"Registered schema subject={subject} id={schema_id}")

    string_serializer = StringSerializer("utf_8")

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj,
    )

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 20,
            "batch.num.messages": 1000,
        }
    )

    last_event_by_account: Dict[str, Dict[str, Any]] = {}

    interval = 1.0 / max(EVENTS_PER_SEC, 0.001)

    while True:
        now = utc_now()

        # Decide if we emit a duplicate
        if last_event_by_account and random.random() < DUPLICATE_RATE:
            account_id = random.choice(list(last_event_by_account.keys()))
            dup = last_event_by_account[account_id]
            # Re-send exact same event_id (true duplicate)
            event = dict(dup)
        else:
            event = build_event(now)
            last_event_by_account[event["account_id"]] = dict(event)

        key = event["account_id"]

        try:
            producer.produce(
                topic=TOPIC,
                key=string_serializer(key),
                value=avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
        except Exception as e:
            # If serialization/produce fails, send raw JSON to DLQ for inspection
            print(f"PRODUCE_ERROR sending to DLQ: {e}")
            producer.produce(
                topic=DLQ_TOPIC,
                key=string_serializer(key),
                value=json.dumps({"error": str(e), "event": event}).encode("utf-8"),
                on_delivery=delivery_report,
            )

        producer.poll(0)
        time.sleep(interval)


if __name__ == "__main__":
    main()