# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Payments Data
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads raw payment data from Bronze
# MAGIC 2. Validates payment types and amounts
# MAGIC 3. Aggregates multiple payment installments per order
# MAGIC 4. Writes to Silver as Delta table
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Aggregation functions (`groupBy`, `agg`, `sum`)
# MAGIC - Data validation with business rules
# MAGIC - Handling multi-row records (payments per order)

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, sum as spark_sum, count as spark_count,
    max as spark_max, round as spark_round, when, lit
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data

# COMMAND ----------

df_payments_raw = read_bronze_parquet("payments")
display(df_payments_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Cleansing

# COMMAND ----------

df_payments_clean = (
    df_payments_raw
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("payment_type", trim(upper(col("payment_type"))))
    .withColumn("payment_sequential", col("payment_sequential").cast("int"))
    .withColumn("payment_installments", col("payment_installments").cast("int"))
    .withColumn("payment_value", col("payment_value").cast("double"))
    # Remove invalid records
    .filter(col("order_id").isNotNull())
    .filter(col("payment_value") >= 0)  # No negative payments
    .filter(col("payment_type").isin(["CREDIT_CARD", "BOLETO", "VOUCHER", "DEBIT_CARD", "NOT_DEFINED"]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Aggregate Payment Details Per Order
# MAGIC
# MAGIC An order can have multiple payment records (split payments). We create
# MAGIC both a detail table and a summary table.

# COMMAND ----------

# Payment detail — keep individual payment rows
df_payment_detail = add_metadata_columns(df_payments_clean, source_name="olist_payments")

upsert_to_delta(
    source_df=df_payment_detail,
    target_path="/mnt/silver/payment_details",
    merge_keys=["order_id", "payment_sequential"]
)

# COMMAND ----------

# Payment summary — one row per order with aggregated metrics
df_payment_summary = (
    df_payments_clean
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("total_payment_value"),
        spark_count("*").alias("payment_count"),
        spark_max("payment_installments").alias("max_installments"),
        # Determine primary payment type (highest value)
        spark_max(
            when(col("payment_type") == "CREDIT_CARD", col("payment_value"))
        ).alias("credit_card_amount"),
        spark_max(
            when(col("payment_type") == "BOLETO", col("payment_value"))
        ).alias("boleto_amount"),
        spark_max(
            when(col("payment_type") == "VOUCHER", col("payment_value"))
        ).alias("voucher_amount"),
        spark_max(
            when(col("payment_type") == "DEBIT_CARD", col("payment_value"))
        ).alias("debit_card_amount"),
    )
    .withColumn("total_payment_value", spark_round(col("total_payment_value"), 2))
)

df_payment_summary_silver = add_metadata_columns(df_payment_summary, source_name="olist_payments_summary")

upsert_to_delta(
    source_df=df_payment_summary_silver,
    target_path="/mnt/silver/payment_summary",
    merge_keys=["order_id"]
)

# COMMAND ----------

# Payment type distribution
display(
    df_payments_clean
    .groupBy("payment_type")
    .agg(
        spark_count("*").alias("count"),
        spark_round(spark_sum("payment_value"), 2).alias("total_value")
    )
    .orderBy(col("total_value").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Aggregations** — `groupBy()`, `agg()`, `sum()`, `count()`
# MAGIC 2. **Conditional aggregations** — `when()` inside `agg()`
# MAGIC 3. **Detail vs Summary tables** — Different granularity levels
# MAGIC 4. **Business validation** — Non-negative amounts, valid payment types
