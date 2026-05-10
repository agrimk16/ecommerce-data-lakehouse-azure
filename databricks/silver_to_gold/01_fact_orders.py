# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Fact Orders (Star Schema)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads cleansed data from Silver layer (orders, order_items, payments)
# MAGIC 2. Joins them into a denormalized fact table
# MAGIC 3. Creates `fact_orders` — the central fact table in our Star Schema
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Star Schema fact table design
# MAGIC - Multi-table joins in PySpark
# MAGIC - Surrogate key generation
# MAGIC - Fact table grain definition
# MAGIC
# MAGIC ## Star Schema Design
# MAGIC ```
# MAGIC                    ┌──────────────┐
# MAGIC                    │  dim_date    │
# MAGIC                    └──────┬───────┘
# MAGIC                           │
# MAGIC ┌──────────────┐  ┌──────▼───────┐  ┌──────────────┐
# MAGIC │dim_customers │──│ fact_orders   │──│ dim_products  │
# MAGIC └──────────────┘  └──────┬───────┘  └──────────────┘
# MAGIC                           │
# MAGIC                    ┌──────▼───────┐
# MAGIC                    │dim_geography │
# MAGIC                    └──────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as spark_sum, count as spark_count, avg as spark_avg,
    round as spark_round, year, month, dayofmonth, date_format,
    monotonically_increasing_id
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Silver Tables

# COMMAND ----------

# Read Silver layer Delta tables
df_orders = spark.read.format("delta").load("/mnt/silver/orders")
df_payments = spark.read.format("delta").load("/mnt/silver/payment_summary")

# Read order_items from Silver (run 05_order_items_bronze_to_silver first)
# Falls back to Bronze if Silver isn't available yet
try:
    df_order_items = spark.read.format("delta").load("/mnt/silver/order_items")
    print("  ✓ Reading order_items from Silver layer")
except Exception:
    print("  ⚠ Silver/order_items not found — falling back to Bronze (run 05_order_items_bronze_to_silver first)")
    df_order_items = read_bronze_parquet("order_items")

print(f"Orders:         {df_orders.count():,} records")
print(f"Payment Summary: {df_payments.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Fact Table
# MAGIC
# MAGIC **Grain**: One row per `order_id` (order-level fact)
# MAGIC
# MAGIC **Measures** (numeric facts):
# MAGIC - `total_order_value` — sum of all item prices
# MAGIC - `total_freight_value` — sum of shipping costs
# MAGIC - `total_payment_value` — actual amount paid
# MAGIC - `item_count` — number of items in order
# MAGIC - `delivery_days` — days from purchase to delivery
# MAGIC
# MAGIC **Foreign Keys** (links to dimensions):
# MAGIC - `customer_id` → `dim_customers`
# MAGIC - `order_date_key` → `dim_date`

# COMMAND ----------

# Aggregate order items to order level
df_order_items_agg = (
    df_order_items
    .groupBy("order_id")
    .agg(
        spark_sum("price").alias("total_item_value"),
        spark_sum("freight_value").alias("total_freight_value"),
        spark_count("*").alias("item_count")
    )
    .withColumn("total_item_value", spark_round(col("total_item_value"), 2))
    .withColumn("total_freight_value", spark_round(col("total_freight_value"), 2))
)

# COMMAND ----------

# Join orders + items + payments
df_fact_orders = (
    df_orders
    .join(df_order_items_agg, "order_id", "left")
    .join(df_payments, "order_id", "left")
    .select(
        # Surrogate key
        monotonically_increasing_id().alias("order_sk"),
        # Natural key
        col("order_id"),
        # Foreign keys
        col("customer_id"),
        date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_key"),
        # Measures
        col("total_item_value"),
        col("total_freight_value"),
        col("total_payment_value"),
        col("item_count"),
        col("payment_count"),
        col("max_installments"),
        col("delivery_days"),
        # Degenerate dimensions (facts that act as dimensions)
        col("order_status"),
        col("is_late_delivery"),
        # Timestamps
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
    )
)

print(f"Fact Orders: {df_fact_orders.count():,} records")
df_fact_orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Gold Layer

# COMMAND ----------

# Write fact table to Gold
(
    df_fact_orders
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date_key")
    .save("/mnt/gold/fact_orders")
)

print("✓ fact_orders written to Gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Delta Table Reference (for SQL access)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_orders
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/fact_orders';

# COMMAND ----------

# Quick business metrics
display(
    spark.read.format("delta").load("/mnt/gold/fact_orders")
    .groupBy("order_status")
    .agg(
        spark_count("*").alias("order_count"),
        spark_round(spark_sum("total_payment_value"), 2).alias("total_revenue"),
        spark_round(spark_avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .orderBy(col("total_revenue").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Star Schema design** — Fact table with measures + foreign keys
# MAGIC 2. **Fact table grain** — One row per order (order-level grain)
# MAGIC 3. **Degenerate dimensions** — `order_status` stored directly in fact
# MAGIC 4. **Surrogate keys** — `monotonically_increasing_id()`
# MAGIC 5. **Date keys** — Integer keys (20260115) linking to dim_date
# MAGIC 6. **Multi-table joins** — Orders + Items + Payments
# MAGIC 7. **Partitioning** — By `order_date_key` for query performance
