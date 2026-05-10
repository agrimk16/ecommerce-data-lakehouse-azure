# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Dimension — Customers (SCD Type 2)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads cleansed customer data from Silver
# MAGIC 2. Implements **SCD Type 2** (Slowly Changing Dimension) to track customer changes over time
# MAGIC 3. Creates `dim_customers` in the Gold layer
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - SCD Type 2 implementation in Delta Lake
# MAGIC - Effective date ranges (`valid_from`, `valid_to`)
# MAGIC - `is_current` flag pattern
# MAGIC - Why SCD matters for data warehousing

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, lit, when, coalesce, max as spark_max,
    monotonically_increasing_id, sha2, concat_ws
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is SCD Type 2?
# MAGIC
# MAGIC **Slowly Changing Dimension Type 2** keeps full history of changes.
# MAGIC When a customer's city changes, we:
# MAGIC 1. Mark the old record as `is_current = false` with `valid_to = today`
# MAGIC 2. Insert a new record with `is_current = true` and `valid_to = null`
# MAGIC
# MAGIC This allows analysts to see what a customer's attributes were **at any point in time**.
# MAGIC
# MAGIC | customer_sk | customer_id | city | is_current | valid_from | valid_to |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 1 | C001 | Sao Paulo | false | 2024-01-01 | 2025-06-15 |
# MAGIC | 2 | C001 | Rio de Janeiro | true | 2025-06-15 | null |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Silver Customers

# COMMAND ----------

df_customers_silver = spark.read.format("delta").load("/mnt/silver/customers")

# Select dimension attributes
df_customers_incoming = (
    df_customers_silver
    .select(
        col("customer_unique_id"),
        col("customer_id"),
        col("customer_city"),
        col("customer_state"),
        col("customer_zip_code_prefix"),
    )
    .dropDuplicates(["customer_unique_id"])
    .withColumn("record_hash", sha2(
        concat_ws("||",
            col("customer_city"),
            col("customer_state"),
            col("customer_zip_code_prefix")
        ), 256
    ))
)

print(f"Incoming customers: {df_customers_incoming.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: SCD Type 2 Implementation

# COMMAND ----------

gold_path = "/mnt/gold/dim_customers"

try:
    # Load existing dimension
    dim_customers = DeltaTable.forPath(spark, gold_path)

    # --- Step A: Find changed records ---
    df_existing = dim_customers.toDF().filter(col("is_current") == True)

    df_changes = (
        df_customers_incoming.alias("incoming")
        .join(
            df_existing.alias("existing"),
            col("incoming.customer_unique_id") == col("existing.customer_unique_id"),
            "inner"
        )
        .filter(col("incoming.record_hash") != col("existing.record_hash"))
        .select("incoming.*")
    )

    # --- Step B: Find new records (not in dimension) ---
    df_new = (
        df_customers_incoming.alias("incoming")
        .join(
            df_existing.alias("existing"),
            col("incoming.customer_unique_id") == col("existing.customer_unique_id"),
            "left_anti"
        )
    )

    changed_count = df_changes.count()
    new_count = df_new.count()
    print(f"Changed records: {changed_count:,}")
    print(f"New records:     {new_count:,}")

    if changed_count > 0:
        # --- Step C: Expire old records ---
        (
            dim_customers.alias("target")
            .merge(
                df_changes.alias("source"),
                "target.customer_unique_id = source.customer_unique_id AND target.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": lit(False),
                "valid_to": current_timestamp()
            })
            .execute()
        )

        # --- Step D: Insert new versions ---
        df_new_versions = (
            df_changes
            .withColumn("customer_sk", monotonically_increasing_id())
            .withColumn("is_current", lit(True))
            .withColumn("valid_from", current_timestamp())
            .withColumn("valid_to", lit(None).cast("timestamp"))
        )
        df_new_versions.write.format("delta").mode("append").save(gold_path)

    if new_count > 0:
        # Insert brand new customers
        df_new_customers = (
            df_new
            .withColumn("customer_sk", monotonically_increasing_id())
            .withColumn("is_current", lit(True))
            .withColumn("valid_from", current_timestamp())
            .withColumn("valid_to", lit(None).cast("timestamp"))
        )
        df_new_customers.write.format("delta").mode("append").save(gold_path)

    print("✓ SCD Type 2 merge completed")

except Exception as e:
    # First load — create the dimension
    print(f"Creating new dim_customers table...")
    df_initial = (
        df_customers_incoming
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn("is_current", lit(True))
        .withColumn("valid_from", current_timestamp())
        .withColumn("valid_to", lit(None).cast("timestamp"))
    )
    df_initial.write.format("delta").mode("overwrite").save(gold_path)
    print(f"✓ Created dim_customers with {df_initial.count():,} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_customers
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dim_customers';

# COMMAND ----------

# Verify SCD Type 2
display(spark.read.format("delta").load(gold_path).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **SCD Type 2** — Full history tracking with `valid_from`/`valid_to`
# MAGIC 2. **`is_current` flag** — Quickly filter to latest version
# MAGIC 3. **Change detection** — Using `record_hash` to find modifications
# MAGIC 4. **Delta Lake MERGE** — Expire old + Insert new in single transaction
# MAGIC 5. **Why this matters** — Enables "as-of" reporting in the warehouse
