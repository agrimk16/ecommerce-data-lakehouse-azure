# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Dimension — Geography
# MAGIC
# MAGIC Creates `dim_geography` from geolocation and customer data.
# MAGIC Enables geographic analysis of orders, delivery times, etc.

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, avg as spark_avg, round as spark_round, count as spark_count,
    monotonically_increasing_id, first
)

# COMMAND ----------

# Read geolocation data (Bronze — it may not have a Silver version)
df_geo = read_bronze_parquet("geolocation")

# Aggregate geolocation to zip code level (avg lat/lon since multiple points per zip)
df_dim_geography = (
    df_geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        spark_round(spark_avg("geolocation_lat"), 6).alias("latitude"),
        spark_round(spark_avg("geolocation_lng"), 6).alias("longitude"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state"),
        spark_count("*").alias("geo_points_count")
    )
    .withColumn("geography_sk", monotonically_increasing_id())
    .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
    .select(
        "geography_sk",
        "zip_code_prefix",
        "city",
        "state",
        "latitude",
        "longitude",
        # Brazilian region mapping
        col("state"),
    )
)

# COMMAND ----------

# Add region based on state
from pyspark.sql.functions import when

df_dim_geography = (
    df_dim_geography
    .withColumn("region",
        when(col("state").isin("SP", "RJ", "MG", "ES"), "Southeast")
        .when(col("state").isin("PR", "SC", "RS"), "South")
        .when(col("state").isin("BA", "PE", "CE", "MA", "PI", "RN", "PB", "SE", "AL"), "Northeast")
        .when(col("state").isin("GO", "MT", "MS", "DF"), "Central-West")
        .when(col("state").isin("AM", "PA", "AC", "RO", "RR", "AP", "TO"), "North")
        .otherwise("Unknown")
    )
)

# Write to Gold
(
    df_dim_geography
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/gold/dim_geography")
)

print(f"✓ dim_geography: {df_dim_geography.count():,} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_geography
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dim_geography';

# COMMAND ----------

# Orders by region
display(
    df_dim_geography
    .groupBy("region")
    .agg(spark_count("*").alias("zip_codes"))
    .orderBy(col("zip_codes").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Geography dimension** — Zip-level analysis with lat/lon
# MAGIC 2. **Aggregating granularity** — Multiple geo points → one per zip code
# MAGIC 3. **Business logic** — State-to-region mapping
# MAGIC 4. **Enabling geo-analysis** — Power BI map visualizations
