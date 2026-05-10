# Step 6: Pipeline Orchestration with Azure Data Factory

## Concepts

Azure Data Factory (ADF) orchestrates the end-to-end pipeline — triggering Databricks notebooks in the correct order, with dependency handling, retry logic, and monitoring.

**Key Interview Concepts:**

- **Linked Services**: Connection credentials to external systems (Databricks, ADLS, Key Vault)
- **Datasets**: Pointers to specific data locations — used by copy activities
- **Pipelines**: Ordered sequences of activities with dependency chains
- **Master Pipeline**: A single pipeline that calls sub-pipelines in sequence — simplifies scheduling
- **Triggers**: Schedule-based or event-based execution (e.g., daily at 6 AM, or when a file arrives)

## 6.1 Grant ADF Permissions

ADF needs access to storage and Databricks:

```bash
source setup/variables.sh

# Get ADF managed identity
ADF_PRINCIPAL_ID=$(az datafactory show \
    --name $DATA_FACTORY \
    --resource-group $RESOURCE_GROUP \
    --query identity.principalId -o tsv)

# Storage Blob Data Contributor on ADLS
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee "$ADF_PRINCIPAL_ID" \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

# Contributor on Databricks workspace
az role assignment create \
    --role "Contributor" \
    --assignee "$ADF_PRINCIPAL_ID" \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Databricks/workspaces/$DATABRICKS_WORKSPACE"
```

## 6.2 Create Linked Services

In ADF Studio → **Manage** → **Linked Services** → **+ New**:

### Azure Key Vault

| Setting     | Value                                         |
| ----------- | --------------------------------------------- |
| Name        | `ls_akv_ecommerce`                            |
| Base URL    | `https://ecomlh-kv-<suffix>.vault.azure.net/` |
| Auth method | Managed Identity                              |

### ADLS Gen2

| Setting     | Value                                            |
| ----------- | ------------------------------------------------ |
| Name        | `ls_adls_ecommerce`                              |
| URL         | `https://ecomlhdl<suffix>.dfs.core.windows.net/` |
| Auth method | Managed Identity                                 |

### Azure Databricks

| Setting        | Value                                    |
| -------------- | ---------------------------------------- |
| Name           | `ls_databricks_ecommerce`                |
| Workspace URL  | From Databricks workspace overview       |
| Cluster ID     | From Databricks → Compute → cluster JSON |
| Authentication | Managed Identity                         |

## 6.3 Create Datasets

Create datasets for source and sink locations. Each dataset maps to a specific ADLS path:

| Dataset Name          | Type          | Path                |
| --------------------- | ------------- | ------------------- |
| `ds_bronze_ecommerce` | DelimitedText | `bronze/ecommerce/` |
| `ds_silver_delta`     | Parquet       | `silver/`           |
| `ds_gold_delta`       | Parquet       | `gold/`             |
| `ds_config`           | JSON          | `config/`           |

JSON definitions are in `adf/datasets/`.

## 6.4 Create Pipelines

### Pipeline 1: Bronze → Silver

`pl_bronze_to_silver` — Runs the 5 cleansing notebooks in sequence:

```
Notebook Activity: customers_bronze_to_silver
    └─► Notebook Activity: orders_bronze_to_silver
        └─► Notebook Activity: order_items_bronze_to_silver
            └─► Notebook Activity: payments_bronze_to_silver
                └─► Notebook Activity: products_bronze_to_silver
```

Each activity:

- **Type**: Databricks Notebook
- **Linked Service**: `ls_databricks_ecommerce`
- **Notebook Path**: `/Users/<your-email>/bronze_to_silver/<notebook_name>`
- **Dependency**: Success of previous activity

### Pipeline 2: Silver → Gold

`pl_silver_to_gold` — Runs the 5 dimensional modeling notebooks:

```
Notebook Activity: dim_customers
    ├─► Notebook Activity: dim_products     (parallel)
    ├─► Notebook Activity: dim_sellers      (parallel)
    └─► Notebook Activity: dim_date         (parallel)
            └─► Notebook Activity: fact_order_items  (after all dims)
```

> **Design Decision:** Dimension notebooks can run in parallel, but the fact table must wait for all dimensions.

### Pipeline 3: Master Pipeline

`pl_master_ecommerce` — Orchestrates sub-pipelines:

```
Execute Pipeline: pl_bronze_to_silver
    └─► Execute Pipeline: pl_silver_to_gold
```

## 6.5 Create a Schedule Trigger

1. ADF Studio → **Manage** → **Triggers** → **+ New**
2. Configure:

| Setting    | Value                    |
| ---------- | ------------------------ |
| Name       | `tr_daily_ecommerce`     |
| Type       | Schedule                 |
| Recurrence | Every 1 day at 06:00 UTC |
| Pipeline   | `pl_master_ecommerce`    |
| Start      | Tomorrow's date          |

3. Click **Publish All** to deploy

> **Cost Tip:** Leave the trigger in **Stopped** state when not actively testing. ADF charges per activity run.

## 6.6 Test the Pipeline

1. Open `pl_master_ecommerce` → **Add trigger** → **Trigger now**
2. Monitor in **Monitor** → **Pipeline runs**
3. Click into the run to see each activity's status and duration
4. Expected total runtime: 10–20 minutes (depending on cluster startup)

## 6.7 Troubleshooting

**"Cluster not found" error:**
Ensure the cluster ID in the Databricks linked service matches an existing cluster. The cluster starts automatically.

**"Access denied" on storage:**
Re-run the role assignment commands in Section 6.1.

**Notebook timeout:**
Increase the timeout in each Databricks Notebook activity (default is 7200 seconds / 2 hours).

## What's Next?

Proceed to [07_synapse_analytics.md](07_synapse_analytics.md) to query the Gold layer using Synapse Serverless SQL and build Power BI dashboards.
