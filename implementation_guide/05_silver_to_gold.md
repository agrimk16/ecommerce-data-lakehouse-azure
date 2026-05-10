# Step 5: Silver → Gold (Star Schema)

## Concepts

The Gold layer contains **business-ready, dimensional models** optimized for analytics. This project implements a Star Schema with one fact table and four dimension tables, including SCD Type 2 for slowly changing dimensions.

**Key Interview Concepts:**

- **Star Schema**: Central fact table linked to dimension tables via foreign keys — optimized for analytical queries
- **Fact Table**: Stores measurable business events (order items with revenue, quantities, shipping metrics)
- **Dimension Tables**: Descriptive attributes (customer info, product details, date attributes, seller geography)
- **SCD Type 2**: Tracks historical changes by adding `valid_from`, `valid_to`, and `is_current` columns — preserves complete history
- **Delta Lake MERGE**: Enables upserts — new records are inserted, changed records create new versions

## 5.1 Notebook Execution Order

Run these 5 notebooks in sequence — **dimensions first, then fact**:

| Order | Notebook                          | Output (Gold Delta)           | Records |
| ----- | --------------------------------- | ----------------------------- | ------- |
| 1     | `silver_to_gold/dim_customers`    | `/mnt/gold/dim_customers/`    | ~99K    |
| 2     | `silver_to_gold/dim_products`     | `/mnt/gold/dim_products/`     | ~33K    |
| 3     | `silver_to_gold/dim_sellers`      | `/mnt/gold/dim_sellers/`      | ~3K     |
| 4     | `silver_to_gold/dim_date`         | `/mnt/gold/dim_date/`         | ~730    |
| 5     | `silver_to_gold/fact_order_items` | `/mnt/gold/fact_order_items/` | ~113K   |

> **Why dimensions first?** The fact table references dimension keys. Building dimensions first ensures referential consistency.

## 5.2 Star Schema Design

```
                    ┌──────────────────┐
                    │   dim_customers  │
                    │──────────────────│
                    │ customer_key (PK)│
                    │ customer_id      │
                    │ city, state      │
                    │ valid_from/to    │
                    │ is_current       │
                    └────────┬─────────┘
                             │
┌──────────────┐    ┌────────┴─────────┐    ┌──────────────┐
│ dim_products │    │ fact_order_items  │    │  dim_sellers │
│──────────────│    │──────────────────│    │──────────────│
│ product_key  │◄───│ product_key (FK) │    │ seller_key   │
│ product_id   │    │ customer_key (FK)│───►│ seller_id    │
│ category     │    │ seller_key (FK)  │    │ city, state  │
│ name_length  │    │ date_key (FK)    │    └──────────────┘
│ weight, size │    │ order_id         │
└──────────────┘    │ price, freight   │    ┌──────────────┐
                    │ quantity         │    │   dim_date   │
                    │ delivery_days    │───►│──────────────│
                    └──────────────────┘    │ date_key     │
                                           │ year, month  │
                                           │ quarter, dow │
                                           │ is_weekend   │
                                           └──────────────┘
```

## 5.3 SCD Type 2 Implementation

The `dim_customers` notebook implements SCD Type 2 using Delta Lake MERGE:

```python
# Simplified MERGE logic for SCD Type 2
deltaTable.alias("target").merge(
    new_data.alias("source"),
    "target.customer_id = source.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.city != source.city OR target.state != source.state",
    set={
        "is_current": "false",
        "valid_to": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "source.customer_id",
        "city": "source.city",
        "state": "source.state",
        "is_current": "true",
        "valid_from": "current_timestamp()",
        "valid_to": "lit('9999-12-31')"
    }
).execute()
```

**Interview Talking Point:** "When a customer moves to a new city, the old record is closed with `is_current=false` and a new record is inserted. This lets us report historical orders against the customer's address _at the time of the order_, not their current address."

## 5.4 Verify Gold Output

```python
for table in ["dim_customers", "dim_products", "dim_sellers", "dim_date", "fact_order_items"]:
    df = spark.read.format("delta").load(f"/mnt/gold/{table}/")
    print(f"{table}: {df.count()} rows, {len(df.columns)} columns")
```

### Quick Analytics Validation

```python
# Revenue by product category — confirms joins work correctly
fact = spark.read.format("delta").load("/mnt/gold/fact_order_items/")
products = spark.read.format("delta").load("/mnt/gold/dim_products/")

fact.join(products, "product_key") \
    .groupBy("category_name") \
    .agg({"price": "sum"}) \
    .orderBy("sum(price)", ascending=False) \
    .show(10)
```

## 5.5 Troubleshooting

**"AnalysisException: Table or view not found":**
Ensure all Silver tables exist from Step 4 before running Gold notebooks.

**Fact table has fewer rows than expected:**
The fact table performs inner joins to dimensions. Missing dimension records cause dropped rows — re-run dimensions first.

**SCD Type 2 duplicates:**
If you re-run `dim_customers` multiple times, closed records accumulate. Drop and recreate if testing: `dbutils.fs.rm("/mnt/gold/dim_customers/", True)`

## What's Next?

Proceed to [06_pipeline_orchestration.md](06_pipeline_orchestration.md) to automate the full pipeline with Azure Data Factory.
