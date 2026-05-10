# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Customers Data
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads raw customers data from Bronze
# MAGIC 2. Standardizes city/state names
# MAGIC 3. Handles duplicates (same customer, multiple unique IDs)
# MAGIC 4. Writes to Silver as Delta table
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - String manipulation in PySpark
# MAGIC - Window functions for deduplication
# MAGIC - Understanding surrogate keys vs natural keys

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, initcap, regexp_replace,
    row_number
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data

# COMMAND ----------

df_customers_raw = read_bronze_parquet("customers")
display(df_customers_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Cleansing

# COMMAND ----------

df_customers_clean = (
    df_customers_raw
    # Trim whitespace
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_unique_id", trim(col("customer_unique_id")))
    # Standardize city names (InitCap, remove special chars)
    .withColumn("customer_city", initcap(trim(col("customer_city"))))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[^a-zA-Z\\s]", ""))
    # Standardize state (uppercase, 2 chars)
    .withColumn("customer_state", trim(col("customer_state")).substr(1, 2))
    # Clean zip code (first 5 digits)
    .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix")))
    # Remove nulls on primary key
    .filter(col("customer_id").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Handle Duplicate Customers
# MAGIC
# MAGIC In the Olist dataset, the same physical customer (`customer_unique_id`)
# MAGIC can have multiple `customer_id` values (one per order). We keep all
# MAGIC customer_id mappings but deduplicate exact rows.

# COMMAND ----------

# Remove exact duplicates
df_customers_dedup = df_customers_clean.dropDuplicates(["customer_id"])

# Stats
unique_customers = df_customers_dedup.select("customer_unique_id").distinct().count()
total_ids = df_customers_dedup.count()
print(f"Unique physical customers: {unique_customers:,}")
print(f"Total customer IDs:        {total_ids:,}")
print(f"Avg orders per customer:   {total_ids / unique_customers:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Silver

# COMMAND ----------

df_customers_silver = add_metadata_columns(df_customers_dedup, source_name="olist_customers")

upsert_to_delta(
    source_df=df_customers_silver,
    target_path="/mnt/silver/customers",
    merge_keys=["customer_id"]
)

# COMMAND ----------

# Verify
get_table_stats("/mnt/silver/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **String standardization** — `initcap()`, `regexp_replace()`, `trim()`
# MAGIC 2. **Surrogate vs natural keys** — `customer_id` vs `customer_unique_id`
# MAGIC 3. **Deduplication strategies** — `dropDuplicates()`, Window functions
# MAGIC 4. **Delta Lake MERGE** — Idempotent writes for incremental loads
