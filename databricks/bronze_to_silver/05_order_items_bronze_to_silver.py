# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Order Items Data
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads raw order_items data from Bronze layer (Parquet)
# MAGIC 2. Validates price and freight values
# MAGIC 3. Casts data types and trims strings
# MAGIC 4. Removes duplicates
# MAGIC 5. Writes cleansed data to Silver layer as Delta Lake table
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Handling numeric data validation in PySpark
# MAGIC - Composite primary key deduplication
# MAGIC - Delta Lake MERGE with composite keys
# MAGIC
# MAGIC **Schema (Olist order_items):**
# MAGIC ```
# MAGIC order_id          — FK to orders table
# MAGIC order_item_id     — line item number within the order (1, 2, 3...)
# MAGIC product_id        — FK to products table
# MAGIC seller_id         — FK to sellers table
# MAGIC shipping_limit_date
# MAGIC price             — item price (BRL)
# MAGIC freight_value     — shipping cost for this item
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, round as spark_round, to_timestamp, when
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Data from Bronze

# COMMAND ----------

df_items_raw = read_bronze_parquet("order_items")
display(df_items_raw.limit(10))

# COMMAND ----------

# Raw data profile
print("Raw Data Profile:")
print(f"  Total records:        {df_items_raw.count():,}")
print(f"  Distinct order IDs:   {df_items_raw.select('order_id').distinct().count():,}")
print(f"  Distinct product IDs: {df_items_raw.select('product_id').distinct().count():,}")

# Check null counts on key columns
for c in ["order_id", "product_id", "seller_id", "price", "freight_value"]:
    null_count = df_items_raw.filter(col(c).isNull()).count()
    print(f"  Null '{c}': {null_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Schema Enforcement & Type Casting

# COMMAND ----------

df_items_typed = (
    df_items_raw
    # Trim string keys
    .withColumn("order_id",    trim(col("order_id")))
    .withColumn("product_id",  trim(col("product_id")))
    .withColumn("seller_id",   trim(col("seller_id")))
    # Cast numeric types
    .withColumn("order_item_id",   col("order_item_id").cast("int"))
    .withColumn("price",           col("price").cast("double"))
    .withColumn("freight_value",   col("freight_value").cast("double"))
    # Round monetary values to 2 decimal places
    .withColumn("price",         spark_round(col("price"), 2))
    .withColumn("freight_value", spark_round(col("freight_value"), 2))
    # Cast timestamp
    .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
)

print("Schema after type casting:")
df_items_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Cleansing & Validation

# COMMAND ----------

# Remove records with null primary keys
df_items_clean = (
    df_items_typed
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("order_item_id").isNotNull())
)

# Remove records with negative or extreme price values (data quality)
df_items_valid = (
    df_items_clean
    .filter(col("price") >= 0)
    .filter(col("freight_value") >= 0)
    .filter(col("price") < 100000)      # sanity upper bound: R$100k per item
)

# Remove exact duplicates on composite primary key (order_id + order_item_id)
df_items_dedup = df_items_valid.dropDuplicates(["order_id", "order_item_id"])

# Add derived column: total line value (price + freight)
df_items_enriched = df_items_dedup.withColumn(
    "line_total_value",
    spark_round(col("price") + col("freight_value"), 2)
)

# Quality report
raw_count = df_items_raw.count()
clean_count = df_items_enriched.count()
print(f"Data Quality Report:")
print(f"  Raw records:      {raw_count:,}")
print(f"  After cleansing:  {clean_count:,}")
print(f"  Records removed:  {raw_count - clean_count:,}")
print(f"  Removal rate:     {((raw_count - clean_count) / raw_count) * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Metadata & Write to Silver (Delta Lake)

# COMMAND ----------

df_items_silver = add_metadata_columns(df_items_enriched, source_name="olist_order_items")

# UPSERT into Silver Delta table
# Composite key: order_id + order_item_id uniquely identifies a line item
silver_path = "/mnt/silver/order_items"

upsert_to_delta(
    source_df=df_items_silver,
    target_path=silver_path,
    merge_keys=["order_id", "order_item_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Output

# COMMAND ----------

df_silver_verify = spark.read.format("delta").load(silver_path)
display(df_silver_verify.limit(10))

# COMMAND ----------

get_table_stats(silver_path)

# COMMAND ----------

# Preview: top-selling categories by line items
from pyspark.sql.functions import count as spark_count, sum as spark_sum, desc

display(
    df_silver_verify
    .groupBy("seller_id")
    .agg(
        spark_count("*").alias("items_sold"),
        spark_sum("price").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Composite primary keys** — `(order_id, order_item_id)` together uniquely identify a row
# MAGIC 2. **Numeric validation** — Filtering negative/extreme values as a data quality step
# MAGIC 3. **Monetary precision** — Rounding doubles to 2 decimal places
# MAGIC 4. **Derived columns** — `line_total_value = price + freight_value`
# MAGIC 5. **Delta MERGE with composite keys** — UPSERT using two-column merge condition
