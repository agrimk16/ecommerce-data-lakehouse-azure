# Delta Lake Guide

## What is Delta Lake?

An open-source storage layer that adds ACID transactions, schema enforcement, and time travel to data lakes. Built on top of Parquet files.

## Why Delta Lake? (Interview Answer)

> "Delta Lake solves the reliability problems of data lakes. It provides ACID transactions for concurrent reads/writes, schema enforcement to prevent bad data, and time travel for auditing and rollbacks. It's the storage format that makes the lakehouse architecture possible."

## Key Features

### 1. ACID Transactions

```python
# Multiple writers can write concurrently without corruption
# Delta uses optimistic concurrency control
df.write.format("delta").mode("append").save(path)
```

### 2. Schema Enforcement

```python
# This will FAIL if df_new has different schema:
df_new.write.format("delta").mode("append").save(existing_delta_path)
# Error: "A]schema mismatch detected..."

# To evolve schema (add new columns):
df_new.write.format("delta").mode("append") \
    .option("mergeSchema", "true").save(path)
```

### 3. MERGE (UPSERT)

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/silver/orders")

(
    delta_table.alias("target")
    .merge(new_data.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()     # Update existing records
    .whenNotMatchedInsertAll()  # Insert new records
    .execute()
)
```

### 4. Time Travel

```python
# Read version 0 (original data)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)

# Read data as of a specific timestamp
df_old = spark.read.format("delta") \
    .option("timestampAsOf", "2026-03-01").load(path)

# View history
delta_table = DeltaTable.forPath(spark, path)
display(delta_table.history())
```

### 5. Z-ORDER Optimization

```python
# Optimize file layout for faster queries on specific columns
delta_table.optimize().executeZOrderBy("order_date_key", "customer_id")
```

### 6. VACUUM (Clean up old files)

```python
# Remove files older than 7 days (default retention)
delta_table.vacuum(168)  # hours
```

## Delta vs Parquet

| Feature            | Parquet           | Delta Lake                |
| ------------------ | ----------------- | ------------------------- |
| ACID transactions  | No                | Yes                       |
| Schema enforcement | No                | Yes                       |
| MERGE (UPSERT)     | No                | Yes                       |
| Time travel        | No                | Yes                       |
| Concurrent writes  | Corrupts data     | Safe                      |
| Small file problem | Manual compaction | Auto-optimize             |
| Schema evolution   | Manual            | Built-in                  |
| File format        | Parquet           | Parquet + transaction log |

## Common Interview Questions

1. **What's the Delta transaction log?**
   - `_delta_log/` folder with JSON files tracking every change
   - Each commit creates a new JSON file (00001.json, 00002.json)
   - Every 10 commits, a checkpoint Parquet file is created

2. **How does MERGE work internally?**
   - Reads source and target
   - Identifies matched/unmatched rows
   - Writes new Parquet files for changes
   - Updates transaction log atomically

3. **What's Z-ORDER?**
   - Co-locates related data in the same files
   - Enables data skipping for faster queries
   - Like a multi-dimensional index
