# Spark Structured Streaming Guide

## What is Structured Streaming?

A stream processing engine built on Spark SQL. Treats a live data stream as an unbounded table that keeps getting new rows appended.

## Why Structured Streaming? (Interview Answer)

> "Structured Streaming lets us process real-time data using the same DataFrame API as batch processing. It provides exactly-once guarantees, handles late data with watermarks, and integrates natively with Delta Lake for reliable streaming pipelines."

## Core Model

```
                            ┌──────────────────────┐
  New data arrives →        │   Unbounded Input     │
                            │   Table               │
                            │   ┌────────────────┐  │
                            │   │ Old data        │  │
                            │   │ + New data      │  │
                            │   └────────────────┘  │
                            └──────────┬───────────┘
                                       │ Query
                                       ▼
                            ┌──────────────────────┐
                            │   Result Table        │
                            │   (updated each       │
                            │    micro-batch)       │
                            └──────────┬───────────┘
                                       │ Output
                                       ▼
                            ┌──────────────────────┐
                            │   Sink (Delta Lake,   │
                            │   Console, Kafka...)  │
                            └──────────────────────┘
```

## Reading from Event Hubs

```python
# Connection string from Key Vault
conn_string = dbutils.secrets.get("keyvault-scope", "eventhub-connection-string")

eh_conf = {
    "eventhubs.connectionString":
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils
        .encrypt(conn_string)
}

# Read stream
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)
```

## Processing the Stream

```python
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

# Define expected schema
schema = StructType() \
    .add("event_type", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("timestamp", TimestampType())

# Parse JSON body
parsed_stream = (
    raw_stream
    .select(
        col("body").cast("string").alias("json_body"),
        col("enqueuedTime").alias("event_time")
    )
    .select(from_json("json_body", schema).alias("data"), "event_time")
    .select("data.*", "event_time")
)
```

## Output Modes

| Mode         | Behavior                      | Use Case                       |
| ------------ | ----------------------------- | ------------------------------ |
| **Append**   | Only new rows written         | Raw event logging              |
| **Complete** | Entire result table rewritten | Aggregations with small output |
| **Update**   | Only changed rows written     | Aggregations to Delta Lake     |

```python
# Append mode — write raw events
(
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/bronze/checkpoints/clickstream")
    .start("/mnt/bronze/clickstream_raw")
)
```

## Windowed Aggregations

```python
# 1-minute tumbling window
windowed = (
    parsed_stream
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window("event_time", "1 minute"),
        "event_type"
    )
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    )
)
```

### Window Types

| Type         | Description            | Code                                    |
| ------------ | ---------------------- | --------------------------------------- |
| **Tumbling** | Fixed, non-overlapping | `window(col, "1 minute")`               |
| **Sliding**  | Fixed, overlapping     | `window(col, "1 minute", "30 seconds")` |
| **Session**  | Gap-based              | `session_window(col, "10 minutes")`     |

## Watermarks (Handling Late Data)

```python
# Accept data up to 5 minutes late
stream.withWatermark("event_time", "5 minutes")
```

- **Without watermark**: Spark keeps all state forever (memory grows)
- **With watermark**: Spark drops state for windows older than threshold
- Late data arriving within the watermark is included
- Late data arriving after the watermark is dropped

## Checkpointing

Checkpoints store stream processing state for fault tolerance:

```python
.option("checkpointLocation", "/mnt/bronze/checkpoints/stream_name")
```

- Stores: offsets processed, state for aggregations
- Enables: exactly-once processing, recovery from failures
- **Never share** checkpoint locations between streams
- **Never delete** checkpoints for a running stream

## Trigger Types

```python
# Process as fast as possible (default)
.trigger(processingTime="0 seconds")

# Process every 30 seconds
.trigger(processingTime="30 seconds")

# Process all available data once, then stop
.trigger(availableNow=True)

# Process one micro-batch, then stop
.trigger(once=True)
```

> **Cost tip**: Use `trigger(availableNow=True)` for near-real-time at lower cost.

## Writing to Delta Lake (Bronze → Silver)

This is exactly what our project does:

```python
# Bronze: Raw append (all events, no processing)
bronze_query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/bronze/checkpoints/clickstream")
    .trigger(processingTime="30 seconds")
    .start("/mnt/bronze/clickstream_raw")
)

# Silver: Windowed aggregations
silver_query = (
    windowed.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "/mnt/silver/checkpoints/clickstream_agg")
    .trigger(processingTime="1 minute")
    .start("/mnt/silver/clickstream_aggregated")
)
```

## Common Interview Questions

1. **Exactly-once vs at-least-once?**
   - Structured Streaming provides exactly-once with checkpointing + Delta Lake
   - Each micro-batch is atomic; on failure it replays from the checkpoint
   - Delta Lake's ACID transactions prevent duplicate writes

2. **How do you handle schema changes in a stream?**
   - Use `option("mergeSchema", "true")` for additive changes
   - For breaking changes: stop stream, update schema, reset checkpoint

3. **Batch vs Streaming — when to use which?**
   - Batch: daily/hourly reporting, ETL, cost-sensitive workloads
   - Streaming: real-time dashboards, fraud detection, IoT telemetry
   - Near-real-time: use `trigger(availableNow=True)` as a middle ground
