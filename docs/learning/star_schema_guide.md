# Star Schema Design Guide

## What is a Star Schema?

A dimensional modeling technique where a central **fact table** connects to surrounding **dimension tables** in a star pattern. Optimized for analytical queries.

## Why Star Schema? (Interview Answer)

> "Star schema simplifies complex analytical queries by separating measurable events (facts) into one table and descriptive context (dimensions) into separate tables. This makes queries faster through reduced joins, intuitive for business users, and optimized for BI tools like Power BI."

## Core Concepts

### Fact Tables

- Store **measurable events** (transactions, events, metrics)
- Contain **foreign keys** to dimension tables
- Contain **measures** (numeric values you aggregate)
- Typically the largest tables

```
fact_orders
├── order_key (surrogate key)
├── date_key (FK → dim_date)
├── customer_key (FK → dim_customers)
├── product_key (FK → dim_products)
├── geography_key (FK → dim_geography)
├── order_amount (measure)
├── freight_value (measure)
├── payment_value (measure)
└── delivery_days (measure)
```

### Dimension Tables

- Store **descriptive attributes** (who, what, where, when)
- Have a **surrogate key** (auto-generated integer)
- Have a **business key** (natural key from source)
- Relatively small, slowly changing

```
dim_customers
├── customer_key (surrogate key - PK)
├── customer_id (business key)
├── customer_city
├── customer_state
├── valid_from
├── valid_to
└── is_current
```

## Grain (Most Important Concept!)

The **grain** defines what one row in the fact table represents.

| Grain                       | Example                     |
| --------------------------- | --------------------------- |
| One row per order           | `fact_orders` - our project |
| One row per order item      | More detail, larger table   |
| One row per day per product | Aggregated summary          |

> **Interview tip**: Always state the grain first when designing a fact table.

## Slowly Changing Dimensions (SCD)

### Type 1: Overwrite

- Simply update the record
- **Loses history**
- Use for: corrections, non-critical attributes

```python
# Before: City = "São Paolo" (typo)
# After:  City = "São Paulo" (corrected)
# Old value is gone
```

### Type 2: Add New Row (Used in This Project)

- Insert new row with updated values
- **Preserves full history**
- Track with: `valid_from`, `valid_to`, `is_current`

```python
# Implementation from our dim_customers notebook:
merge_condition = "target.customer_id = source.customer_id AND target.is_current = true"

(
    dim_table.alias("target")
    .merge(updates.alias("source"), merge_condition)
    .whenMatchedUpdate(
        condition="source.has_changed = true",
        set={
            "valid_to": "source.effective_date",
            "is_current": "lit(false)"
        }
    )
    .whenNotMatchedInsertAll()  # New version with is_current=true
    .execute()
)
```

### Type 3: Add Column

- Add `previous_value` column
- **Limited history** (only one change)
- Rarely used

## Date Dimension

Every star schema needs a date dimension:

```
dim_date
├── date_key (20260301 - integer YYYYMMDD)
├── full_date (2026-03-01 - date type)
├── year (2026)
├── quarter (1)
├── month (3)
├── month_name ("March")
├── day_of_week (1=Monday...7=Sunday)
├── day_name ("Sunday")
├── week_of_year (9)
├── is_weekend (true/false)
└── fiscal_quarter (custom)
```

## Star vs Snowflake Schema

| Feature               | Star                 | Snowflake               |
| --------------------- | -------------------- | ----------------------- |
| Dimension tables      | Denormalized (flat)  | Normalized (sub-tables) |
| Query performance     | Faster (fewer joins) | Slower (more joins)     |
| Storage               | More redundancy      | Less redundancy         |
| Simplicity            | Simple               | Complex                 |
| BI tool compatibility | Excellent            | Good                    |

> **Best practice**: Use star schema unless you have a specific reason for snowflake.

## Common Interview Questions

1. **What's the difference between a surrogate key and a business key?**
   - Surrogate: system-generated integer (customer_key = 1, 2, 3...)
   - Business: natural identifier (customer_id = "abc123")
   - Surrogate keys handle SCD Type 2 (same customer_id has multiple customer_keys)

2. **What's a degenerate dimension?**
   - A dimension stored in the fact table (no separate dimension table)
   - Example: `order_id` in fact_orders — it's descriptive but doesn't warrant its own table

3. **Fact table types?**
   - **Transaction**: One row per event (our fact_orders)
   - **Periodic snapshot**: One row per time period per entity
   - **Accumulating snapshot**: One row per process lifecycle
