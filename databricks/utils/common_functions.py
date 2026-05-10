# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utility Functions
# MAGIC
# MAGIC Shared helper functions used across Bronze→Silver and Silver→Gold notebooks.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws
from delta.tables import DeltaTable

# COMMAND ----------

def add_metadata_columns(df: DataFrame, source_name: str) -> DataFrame:
    """
    Add standard audit/metadata columns to a DataFrame.
    - ingestion_timestamp: when the record was processed
    - source_system: name of the source
    - record_hash: SHA-256 hash for change detection
    """
    columns_for_hash = [c for c in df.columns if c not in ["ingestion_timestamp", "source_system", "record_hash"]]

    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit(source_name))
        .withColumn("record_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in columns_for_hash]), 256))
    )

# COMMAND ----------

def upsert_to_delta(
    source_df: DataFrame,
    target_path: str,
    merge_keys: list,
    partition_cols: list = None
):
    """
    UPSERT (MERGE) a source DataFrame into a Delta table.
    - If the target doesn't exist, creates it.
    - If it exists, merges based on merge_keys.
    """
    # Build merge condition
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    try:
        # Try to load existing Delta table
        target_table = DeltaTable.forPath(spark, target_path)

        # Perform MERGE (UPSERT)
        (
            target_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll(condition="source.record_hash != target.record_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"  ✓ MERGE completed into {target_path}")

    except Exception:
        # Table doesn't exist — create new
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        print(f"  ✓ Created new Delta table at {target_path}")

# COMMAND ----------

def read_bronze_parquet(entity_name: str) -> DataFrame:
    """
    Read the latest Parquet files from the Bronze layer for a given entity.
    """
    bronze_path = f"/mnt/bronze/{entity_name}/"
    df = spark.read.parquet(bronze_path)
    print(f"  ✓ Read {df.count():,} records from Bronze/{entity_name}")
    return df

# COMMAND ----------

def get_table_stats(path: str):
    """
    Print basic stats about a Delta table.
    """
    df = spark.read.format("delta").load(path)
    print(f"  Records: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Schema:")
    df.printSchema()
