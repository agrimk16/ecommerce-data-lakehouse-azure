# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Dimension — Products
# MAGIC
# MAGIC Creates `dim_products` in the Gold layer with product attributes for analysis.

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, monotonically_increasing_id, current_timestamp, lit,
    when, round as spark_round
)

# COMMAND ----------

# Read Silver products
df_products_silver = spark.read.format("delta").load("/mnt/silver/products")

# Build dim_products
df_dim_products = (
    df_products_silver
    .select(
        monotonically_increasing_id().alias("product_sk"),
        col("product_id"),
        col("product_category_name_english").alias("category"),
        col("product_name_lenght").alias("product_name_length"),
        col("product_description_lenght").alias("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        # Derived: volume
        spark_round(
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm"),
            2
        ).alias("product_volume_cm3"),
        # Derived: size category
        when(col("product_weight_g") < 500, "Small")
        .when(col("product_weight_g") < 2000, "Medium")
        .when(col("product_weight_g") < 10000, "Large")
        .otherwise("Extra Large").alias("size_category"),
    )
    .withColumn("valid_from", current_timestamp())
    .withColumn("is_current", lit(True))
)

# Write to Gold
(
    df_dim_products
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/gold/dim_products")
)

print(f"✓ dim_products: {df_dim_products.count():,} records")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_products
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dim_products';

# COMMAND ----------

# Category distribution
display(
    df_dim_products
    .groupBy("category", "size_category")
    .count()
    .orderBy(col("count").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Dimension table design** — descriptive attributes around products
# MAGIC 2. **Derived columns** — `product_volume_cm3`, `size_category`
# MAGIC 3. **SCD Type 1** — Simple overwrite (vs Type 2 for customers)
