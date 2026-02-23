# 02-real-time-analytics-platform
Events flow through Kafka as the system-of-record log. Spark Structured Streaming consumes from Kafka, applies schema enforcement, dedup/idempotency, watermarking + window aggregations, and writes curated datasets to an analytics layer (Parquet). DuckDB queries Parquet for “BI-like” analysis locally. Data quality checks run per micro-batch
