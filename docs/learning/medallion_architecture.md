# Medallion Architecture — Deep Dive

## What is Medallion Architecture?

A data design pattern used by companies like **Microsoft, Databricks, Netflix, and Uber** to organize data in a lakehouse. Data flows through three layers of increasing quality.

## The Three Layers

### Bronze (Raw)

- **Purpose**: Landing zone for raw data exactly as received
- **Format**: CSV, JSON, Parquet (whatever the source provides)
- **Schema**: Inferred/loose — accept everything
- **Quality**: No validation, may contain duplicates and nulls
- **Retention**: Keep forever (audit trail)
- **Who writes**: ADF Copy Activity, Event Hubs Capture

### Silver (Validated)

- **Purpose**: Cleansed, validated, enterprise-ready data
- **Format**: Delta Lake (always)
- **Schema**: Enforced — strict types, not null constraints
- **Quality**: Deduplicated, validated, standardized
- **Retention**: Keep latest + N versions (Delta time travel)
- **Who writes**: Databricks (PySpark)

### Gold (Business)

- **Purpose**: Business-level aggregations, star schema, ML features
- **Format**: Delta Lake
- **Schema**: Star Schema (fact + dimensions)
- **Quality**: Business rules applied, aggregated
- **Retention**: Keep current + history (SCD Type 2)
- **Who reads**: Synapse SQL, Power BI, ML models

## Why Not Just ETL Directly?

| Traditional ETL                   | Medallion Lakehouse                  |
| --------------------------------- | ------------------------------------ |
| Data is transformed once          | Raw data preserved (reprocessable)   |
| Fix bugs = re-extract from source | Fix bugs = reprocess Bronze → Silver |
| Schema changes break everything   | Schema evolution built into Delta    |
| Single format                     | Best format per layer                |
| Hard to debug                     | Each layer is queryable              |

## Implementation Pattern

```python
# Bronze → Silver pattern:
# 1. Read raw Parquet from Bronze
df_raw = spark.read.parquet("/mnt/bronze/orders/")

# 2. Apply cleansing (type casting, null removal, dedup)
df_clean = df_raw.withColumn(...).filter(...).dropDuplicates(...)

# 3. Add metadata (audit columns)
df_silver = add_metadata_columns(df_clean, "orders")

# 4. MERGE into Silver Delta table (idempotent)
upsert_to_delta(df_silver, "/mnt/silver/orders", ["order_id"])
```

```python
# Silver → Gold pattern:
# 1. Read Silver Delta tables
df_orders = spark.read.format("delta").load("/mnt/silver/orders")
df_items = spark.read.format("delta").load("/mnt/silver/order_items")
df_payments = spark.read.format("delta").load("/mnt/silver/payments")

# 2. Join and aggregate into fact/dimension tables
df_fact = df_orders.join(df_items, ...).join(df_payments, ...)

# 3. Write to Gold
df_fact.write.format("delta").save("/mnt/gold/fact_orders")
```

## Real-World Examples

| Company    | Bronze               | Silver                 | Gold                     |
| ---------- | -------------------- | ---------------------- | ------------------------ |
| E-commerce | Raw order JSON       | Clean order records    | Daily sales dashboard    |
| Banking    | Raw transaction logs | Validated transactions | Fraud detection features |
| Healthcare | HL7/FHIR messages    | Patient records        | Clinical trial cohorts   |
| IoT        | Sensor readings      | Filtered & calibrated  | Equipment health scores  |
