# Step 4: Bronze → Silver Transformations

## Concepts

The Silver layer contains **cleansed, typed, and deduplicated** data. Each Bronze CSV file becomes a Delta Lake table in Silver with proper data types, null handling, and consistent column naming.

**Key Interview Concepts:**

- **Delta Lake**: ACID transactions on data lakes — supports MERGE, UPDATE, DELETE, and time travel
- **Schema Enforcement**: Delta tables reject writes that don't match the defined schema
- **Idempotent Writes**: Using `overwrite` mode ensures re-running a notebook produces the same result
- **Data Quality Checks**: Row counts, null checks, and duplicate detection between layers

## 4.1 Notebook Execution Order

Run these 5 notebooks in sequence on your Databricks cluster:

| Order | Notebook                                        | Input (Bronze CSV)                                                     | Output (Silver Delta)      |
| ----- | ----------------------------------------------- | ---------------------------------------------------------------------- | -------------------------- |
| 1     | `bronze_to_silver/customers_bronze_to_silver`   | `olist_customers_dataset.csv`                                          | `/mnt/silver/customers/`   |
| 2     | `bronze_to_silver/orders_bronze_to_silver`      | `olist_orders_dataset.csv`                                             | `/mnt/silver/orders/`      |
| 3     | `bronze_to_silver/order_items_bronze_to_silver` | `olist_order_items_dataset.csv`                                        | `/mnt/silver/order_items/` |
| 4     | `bronze_to_silver/payments_bronze_to_silver`    | `olist_order_payments_dataset.csv`                                     | `/mnt/silver/payments/`    |
| 5     | `bronze_to_silver/products_bronze_to_silver`    | `olist_products_dataset.csv` + `product_category_name_translation.csv` | `/mnt/silver/products/`    |

## 4.2 Running Each Notebook

1. In Databricks → **Workspace** → navigate to the notebook
2. Attach to `ecommerce-cluster`
3. Click **Run All**

Each notebook performs:

1. **Read** — Load CSV from Bronze with `inferSchema=True`
2. **Clean** — Drop duplicates, handle nulls, trim whitespace
3. **Type Cast** — Convert strings to proper types (timestamps, decimals, integers)
4. **Rename** — Standardize column names to `snake_case`
5. **Write** — Save as Delta format to Silver with `overwrite` mode

## 4.3 Verify Silver Output

After running all 5 notebooks, verify in a new notebook cell:

```python
# Check Silver tables exist
for table in ["customers", "orders", "order_items", "payments", "products"]:
    df = spark.read.format("delta").load(f"/mnt/silver/{table}/")
    print(f"{table}: {df.count()} rows, {len(df.columns)} columns")
```

**Expected approximate counts:**

| Table       | Rows     |
| ----------- | -------- |
| customers   | ~99,441  |
| orders      | ~99,441  |
| order_items | ~112,650 |
| payments    | ~103,886 |
| products    | ~32,951  |

## 4.4 Delta Lake Features in Action

### Time Travel

```python
# View history of changes
spark.sql("DESCRIBE HISTORY delta.`/mnt/silver/orders/`").show()

# Read a previous version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/mnt/silver/orders/")
```

### Schema Enforcement

```python
# This would fail if the schema doesn't match the existing Delta table
df_new.write.format("delta").mode("append").save("/mnt/silver/orders/")
```

## 4.5 Troubleshooting

**Notebook fails with "Mount not found":**
Re-run the `mount_storage` notebook from Step 3.

**Row counts are zero:**
Verify CSVs were uploaded to `bronze/ecommerce/` (not just `bronze/`).

**Schema mismatch errors:**
Delete the Silver Delta table folder and re-run the notebook — it uses `overwrite` mode.

## What's Next?

Proceed to [05_silver_to_gold.md](05_silver_to_gold.md) to build the Star Schema with fact and dimension tables.
