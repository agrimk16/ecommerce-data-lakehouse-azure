# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Clickstream Processing with Spark Structured Streaming
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads simulated clickstream events from Azure Event Hubs
# MAGIC 2. Parses JSON event payloads
# MAGIC 3. Applies windowed aggregations (events per minute)
# MAGIC 4. Writes results to Delta Lake (Bronze for raw, Silver for aggregated)
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Spark Structured Streaming
# MAGIC - Event Hubs integration
# MAGIC - Windowed aggregations (tumbling windows)
# MAGIC - Watermarking for late data
# MAGIC - Streaming to Delta Lake (append mode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. Azure Event Hub namespace + hub named `clickstream-events`
# MAGIC 2. Connection string stored in Key Vault as `eventhub-connection-string`
# MAGIC 3. Maven package: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

# COMMAND ----------

from pyspark.sql.functions import (
    col, from_json, window, count as spark_count,
    current_timestamp, to_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Clickstream Event Schema
# MAGIC
# MAGIC Each event represents a user action on the e-commerce site:
# MAGIC - `page_view` — User viewed a product page
# MAGIC - `add_to_cart` — User added item to cart
# MAGIC - `purchase` — User completed purchase
# MAGIC - `search` — User performed a search

# COMMAND ----------

clickstream_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),      # page_view, add_to_cart, purchase, search
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),      # mobile, desktop, tablet
    StructField("event_timestamp", StringType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to Event Hubs

# COMMAND ----------

# Get connection string from Key Vault
key_vault_scope = "ecommerce-kv-scope"
eh_connection_string = dbutils.secrets.get(scope=key_vault_scope, key="eventhub-connection-string")

# Event Hubs configuration
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_string),
    "eventhubs.consumerGroup": "databricks-cg",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read Streaming Data

# COMMAND ----------

# Read from Event Hubs as a streaming DataFrame
df_stream_raw = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

# Parse the body (Event Hubs sends bytes)
df_stream_parsed = (
    df_stream_raw
    .select(
        col("enqueuedTime").alias("enqueued_time"),
        col("offset"),
        col("sequenceNumber").alias("sequence_number"),
        from_json(col("body").cast("string"), clickstream_schema).alias("event")
    )
    .select(
        "enqueued_time",
        "event.*"
    )
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Raw Events to Bronze (Append)

# COMMAND ----------

# Write raw clickstream events to Bronze layer
bronze_stream = (
    df_stream_parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/bronze/clickstream/_checkpoint")
    .start("/mnt/bronze/clickstream/events")
)

print("✓ Bronze stream started — writing raw events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Windowed Aggregations (Real-Time Metrics)
# MAGIC
# MAGIC **Tumbling Window**: Count events per type in 1-minute windows.
# MAGIC
# MAGIC **Watermark**: Allow up to 5 minutes of late data before finalizing a window.

# COMMAND ----------

df_windowed_metrics = (
    df_stream_parsed
    # Watermark: handle events up to 5 min late
    .withWatermark("event_timestamp", "5 minutes")
    # Tumbling window: aggregate every 1 minute
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("event_type"),
        col("device_type")
    )
    .agg(
        spark_count("*").alias("event_count"),
        spark_count("user_id").alias("unique_sessions"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("device_type"),
        col("event_count"),
        col("unique_sessions"),
    )
)

# Write aggregated metrics to Silver
silver_stream = (
    df_windowed_metrics
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/silver/clickstream_metrics/_checkpoint")
    .start("/mnt/silver/clickstream_metrics")
)

print("✓ Silver stream started — windowed metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Monitor Streams

# COMMAND ----------

# Check stream status
for stream in spark.streams.active:
    print(f"Stream: {stream.name}")
    print(f"  Status: {stream.status}")
    print(f"  Recent progress: {stream.recentProgress[-1] if stream.recentProgress else 'No data yet'}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Stop Streams (when done testing)
# MAGIC
# MAGIC **IMPORTANT**: Always stop streams to avoid charges!

# COMMAND ----------

# Uncomment to stop all streams:
# for stream in spark.streams.active:
#     stream.stop()
#     print(f"Stopped stream: {stream.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Spark Structured Streaming** — Continuous data processing
# MAGIC 2. **Event Hubs integration** — Azure's Kafka-compatible event streaming
# MAGIC 3. **`from_json()`** — Parsing JSON payloads from streams
# MAGIC 4. **Tumbling windows** — `window(col, "1 minute")` for time-based aggregation
# MAGIC 5. **Watermarking** — `withWatermark()` to handle late-arriving data
# MAGIC 6. **Checkpointing** — `checkpointLocation` for exactly-once processing
# MAGIC 7. **Streaming to Delta** — Append mode for continuous writes
