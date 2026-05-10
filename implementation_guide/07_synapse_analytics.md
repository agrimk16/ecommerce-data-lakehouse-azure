# Step 7: Synapse Analytics & Power BI

## Concepts

Azure Synapse Serverless SQL queries Delta Lake files directly in ADLS Gen2 — no data loading, no dedicated cluster, pay only per TB scanned. This is the serving layer for BI tools.

**Key Interview Concepts:**

- **Serverless SQL Pool**: On-demand query engine — no provisioning, no idle cost
- **External Tables**: SQL metadata pointing to Delta/Parquet files in the data lake
- **OPENROWSET**: Ad-hoc querying of lake files without creating tables
- **Views**: Reusable query definitions layered on external tables
- **DirectQuery**: Power BI queries Synapse on-demand — no data import/duplication

## 7.1 Grant Synapse Permissions

```bash
source setup/variables.sh

# Get Synapse managed identity
SYNAPSE_MI=$(az synapse workspace show \
    --name $SYNAPSE_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --query identity.principalId -o tsv)

# Storage Blob Data Contributor
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee "$SYNAPSE_MI" \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

# Also grant your own user Synapse SQL Administrator role
# (needed for Power BI DirectQuery)
```

## 7.2 Create External Data Source and File Format

Open Synapse Studio → **Develop** → **SQL script** → **New**. Run against the **Built-in** serverless pool:

```sql
-- Create a database for the Gold layer
CREATE DATABASE ecommerce_gold;
GO

USE ecommerce_gold;
GO

-- External data source pointing to the Gold container
CREATE EXTERNAL DATA SOURCE gold_datalake
WITH (
    LOCATION = 'abfss://gold@ecomlhdl<suffix>.dfs.core.windows.net/'
);
GO

-- File format for Delta/Parquet
CREATE EXTERNAL FILE FORMAT delta_format
WITH (
    FORMAT_TYPE = PARQUET
);
GO
```

Replace `<suffix>` with your unique suffix.

## 7.3 Create External Tables

SQL definitions are in `synapse/external_tables/`. Run each script:

```sql
-- Example: fact_order_items external table
CREATE EXTERNAL TABLE fact_order_items (
    order_id            VARCHAR(50),
    product_key         INT,
    customer_key        INT,
    seller_key          INT,
    date_key            INT,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    quantity            INT,
    delivery_days       INT
)
WITH (
    LOCATION = 'fact_order_items/',
    DATA_SOURCE = gold_datalake,
    FILE_FORMAT = delta_format
);
```

Create external tables for all Gold tables:

| External Table     | Location            |
| ------------------ | ------------------- |
| `fact_order_items` | `fact_order_items/` |
| `dim_customers`    | `dim_customers/`    |
| `dim_products`     | `dim_products/`     |
| `dim_sellers`      | `dim_sellers/`      |
| `dim_date`         | `dim_date/`         |

## 7.4 Create Views

Views in `synapse/views/` provide business-friendly analytics:

```sql
-- Monthly revenue summary
CREATE VIEW vw_monthly_revenue AS
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_revenue,
    SUM(f.freight_value) AS total_freight,
    AVG(f.delivery_days) AS avg_delivery_days
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name;
```

Additional views:

- `vw_revenue_by_category` — Revenue and order counts by product category
- `vw_customer_segments` — Customer lifetime value segments
- `vw_seller_performance` — Seller metrics (revenue, ratings, delivery speed)

## 7.5 Create Stored Procedures

Stored procedures in `synapse/stored_procedures/` support parameterized queries:

```sql
-- Top N products by revenue for a given year
CREATE PROCEDURE sp_top_products_by_revenue
    @year INT,
    @top_n INT
AS
SELECT TOP(@top_n)
    p.category_name,
    p.product_id,
    SUM(f.price) AS total_revenue,
    COUNT(*) AS total_orders
FROM fact_order_items f
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = @year
GROUP BY p.category_name, p.product_id
ORDER BY total_revenue DESC;
```

## 7.6 Test Queries

```sql
-- Quick validation
SELECT COUNT(*) FROM fact_order_items;
SELECT * FROM vw_monthly_revenue ORDER BY year, month;
EXEC sp_top_products_by_revenue @year = 2018, @top_n = 10;
```

## 7.7 Power BI Dashboard

### Connect Power BI to Synapse

1. Open **Power BI Desktop** → **Get Data** → **Azure Synapse Analytics (SQL)**
2. Enter the **Serverless SQL endpoint** (Synapse Studio → Manage → SQL pools → Built-in → Endpoint)
3. Select **DirectQuery** mode
4. Choose the `ecommerce_gold` database
5. Select all views → **Load**

### Recommended Visuals

| Visual                   | Data Source              | Metrics                      |
| ------------------------ | ------------------------ | ---------------------------- |
| Revenue Trend (Line)     | `vw_monthly_revenue`     | Revenue over time            |
| Category Breakdown (Bar) | `vw_revenue_by_category` | Revenue by product category  |
| Delivery KPI (Card)      | `vw_monthly_revenue`     | Average delivery days        |
| Geo Map                  | `dim_customers`          | Order concentration by state |

## 7.8 Troubleshooting

**"External table not found":**
Re-run the external table creation scripts with the correct storage account name.

**"Access denied" from Synapse:**
Re-run the role assignment commands in Section 7.1.

**Power BI cannot connect:**
Ensure your Azure account has the **Synapse SQL Administrator** role on the workspace.

## What's Next?

Proceed to [08_streaming_cicd.md](08_streaming_cicd.md) to add real-time streaming and CI/CD automation.
