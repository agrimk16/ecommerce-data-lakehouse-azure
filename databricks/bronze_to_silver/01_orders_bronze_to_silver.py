# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Orders Data
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads raw orders data from Bronze layer (Parquet)
# MAGIC 2. Applies data quality checks & cleansing
# MAGIC 3. Enforces schema and correct data types
# MAGIC 4. Removes duplicates
# MAGIC 5. Writes cleansed data to Silver layer as Delta Lake table
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - PySpark DataFrame operations
# MAGIC - Delta Lake MERGE (UPSERT)
# MAGIC - Data quality validation
# MAGIC - Schema enforcement

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Data from Bronze

# COMMAND ----------

# Read raw orders from Bronze
df_orders_raw = read_bronze_parquet("orders")

display(df_orders_raw.limit(10))

# COMMAND ----------

# Check raw data quality
print("Raw Data Profile:")
print(f"  Total records: {df_orders_raw.count():,}")
print(f"  Distinct order IDs: {df_orders_raw.select('order_id').distinct().count():,}")
print(f"  Null order_id count: {df_orders_raw.filter(col('order_id').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Schema Enforcement & Type Casting

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, trim, upper, when
from pyspark.sql.types import StringType, TimestampType

df_orders_typed = (
    df_orders_raw
    # Cast timestamps
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date")))
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
    # Trim & standardize strings
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", trim(upper(col("order_status"))))
)

print("Schema after type casting:")
df_orders_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Cleansing & Quality Checks

# COMMAND ----------

# Remove records with null primary key
df_orders_clean = df_orders_typed.filter(col("order_id").isNotNull())

# Remove exact duplicates
df_orders_dedup = df_orders_clean.dropDuplicates(["order_id"])

# Validate order_status (keep only valid statuses)
valid_statuses = ["DELIVERED", "SHIPPED", "PROCESSING", "CANCELED", "UNAVAILABLE",
                  "INVOICED", "CREATED", "APPROVED"]
df_orders_validated = df_orders_dedup.filter(col("order_status").isin(valid_statuses))

# Add derived columns
df_orders_enriched = (
    df_orders_validated
    .withColumn(
        "delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull() & col("order_purchase_timestamp").isNotNull(),
            (col("order_delivered_customer_date").cast("long") - col("order_purchase_timestamp").cast("long")) / 86400
        ).otherwise(None)
    )
    .withColumn(
        "is_late_delivery",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
            True
        ).otherwise(False)
    )
)

# Report
clean_count = df_orders_enriched.count()
raw_count = df_orders_raw.count()
print(f"Data Quality Report:")
print(f"  Raw records:      {raw_count:,}")
print(f"  After cleansing:  {clean_count:,}")
print(f"  Records removed:  {raw_count - clean_count:,}")
print(f"  Removal rate:     {((raw_count - clean_count) / raw_count) * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Metadata & Write to Silver (Delta Lake)

# COMMAND ----------

# Add audit columns
df_orders_silver = add_metadata_columns(df_orders_enriched, source_name="olist_orders")

# UPSERT into Silver Delta table
silver_path = "/mnt/silver/orders"

upsert_to_delta(
    source_df=df_orders_silver,
    target_path=silver_path,
    merge_keys=["order_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Output

# COMMAND ----------

# Read back and verify
df_silver_verify = spark.read.format("delta").load(silver_path)
display(df_silver_verify.limit(10))

# COMMAND ----------

# Delta table history (shows MERGE operations)
delta_table = DeltaTable.forPath(spark, silver_path)
display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **PySpark type casting** — `to_timestamp()`, `trim()`, `upper()`
# MAGIC 2. **Data validation** — null checks, deduplication, valid value filtering
# MAGIC 3. **Derived columns** — `delivery_days`, `is_late_delivery`
# MAGIC 4. **Delta Lake UPSERT** — `MERGE` for idempotent writes
# MAGIC 5. **Delta table history** — version tracking and time travel
