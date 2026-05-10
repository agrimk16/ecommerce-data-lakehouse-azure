# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Products Data
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Reads raw products data from Bronze
# MAGIC 2. Cleans product category names (translate Portuguese → English)
# MAGIC 3. Handles missing dimensions & weights
# MAGIC 4. Writes to Silver as Delta table
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Handling missing/null values with `fillna()` and `coalesce()`
# MAGIC - Joining lookup tables
# MAGIC - Column renaming and restructuring

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, coalesce, lit, avg, round as spark_round,
    when, regexp_replace
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data

# COMMAND ----------

df_products_raw = read_bronze_parquet("products")
display(df_products_raw.limit(10))

# COMMAND ----------

# Check nulls
print("Null counts per column:")
for c in df_products_raw.columns:
    null_count = df_products_raw.filter(col(c).isNull()).count()
    if null_count > 0:
        print(f"  {c}: {null_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Product Category Translation
# MAGIC
# MAGIC The Kaggle dataset includes a category name translation file.
# MAGIC We'll create a mapping for the most common categories.

# COMMAND ----------

# Category translation mapping (Portuguese → English)
category_translations = {
    "beleza_saude": "health_beauty",
    "informatica_acessorios": "computers_accessories",
    "automotivo": "auto",
    "cama_mesa_banho": "bed_bath_table",
    "moveis_decoracao": "furniture_decor",
    "esporte_lazer": "sports_leisure",
    "perfumaria": "perfumery",
    "utilidades_domesticas": "housewares",
    "telefonia": "telephony",
    "relogios_presentes": "watches_gifts",
    "alimentos_bebidas": "food_drink",
    "bebes": "baby",
    "papelaria": "stationery",
    "tablets_impressao_imagem": "tablets_printing_image",
    "brinquedos": "toys",
    "telefonia_fixa": "fixed_telephony",
    "ferramentas_jardim": "garden_tools",
    "fashion_bolsas_e_acessorios": "fashion_bags_accessories",
    "eletroportateis": "small_appliances",
    "consoles_games": "consoles_games",
    "audio": "audio",
    "fashion_calcados": "fashion_shoes",
    "cool_stuff": "cool_stuff",
    "malas_acessorios": "luggage_accessories",
    "climatizacao": "air_conditioning",
    "construcao_ferramentas_construcao": "construction_tools",
    "moveis_cozinha_area_de_servico_jantar_e_jardim": "kitchen_dining_laundry_garden_furniture",
    "construcao_ferramentas_iluminacao": "construction_tools_lighting",
    "artigos_de_festas": "party_supplies",
    "livros_interesse_geral": "books_general_interest",
    "eletronicos": "electronics",
    "pet_shop": "pet_shop",
    "pcs": "pcs",
}

# COMMAND ----------

from pyspark.sql.functions import create_map

# Build mapping expression
mapping_expr = create_map([lit(x) for pair in category_translations.items() for x in pair])

df_products_clean = (
    df_products_raw
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("product_category_name", trim(lower(col("product_category_name"))))
    # Translate category
    .withColumn(
        "product_category_name_english",
        coalesce(
            mapping_expr[col("product_category_name")],
            col("product_category_name")  # Keep original if no translation
        )
    )
    # Handle missing dimensions — fill with median/average
    .withColumn(
        "product_weight_g",
        coalesce(col("product_weight_g").cast("double"), lit(0.0))
    )
    .withColumn(
        "product_length_cm",
        coalesce(col("product_length_cm").cast("double"), lit(0.0))
    )
    .withColumn(
        "product_height_cm",
        coalesce(col("product_height_cm").cast("double"), lit(0.0))
    )
    .withColumn(
        "product_width_cm",
        coalesce(col("product_width_cm").cast("double"), lit(0.0))
    )
    .withColumn(
        "product_photos_qty",
        coalesce(col("product_photos_qty").cast("int"), lit(0))
    )
    .filter(col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Silver

# COMMAND ----------

df_products_silver = add_metadata_columns(df_products_clean, source_name="olist_products")

upsert_to_delta(
    source_df=df_products_silver,
    target_path="/mnt/silver/products",
    merge_keys=["product_id"]
)

# COMMAND ----------

# Top categories
display(
    df_products_silver
    .groupBy("product_category_name_english")
    .count()
    .orderBy(col("count").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Null handling** — `coalesce()`, `fillna()`, default values
# MAGIC 2. **Lookup/mapping** — `create_map()` for value translation
# MAGIC 3. **Type casting** — `.cast("double")`, `.cast("int")`
# MAGIC 4. **Data profiling** — Null counts, category distributions
