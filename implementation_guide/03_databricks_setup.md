# Step 3: Databricks Setup

## Concepts

Azure Databricks is the compute engine for all PySpark transformations in this project. This step configures the workspace so notebooks can securely access the data lake.

**Key Interview Concepts:**

- **Secret Scope**: Databricks-managed reference to Azure Key Vault — notebooks read secrets at runtime without hardcoding credentials
- **DBFS Mounts**: Map ADLS Gen2 containers to `/mnt/` paths so notebooks use simple file paths instead of full `abfss://` URIs
- **Cluster Sizing**: Single-node Standard_DS3_v2 is sufficient for development; autoscaling clusters for production

## 3.1 Launch Databricks Workspace

1. Azure Portal → **Resource Groups** → **rg-ecommerce-lakehouse**
2. Click your Databricks workspace (`ecomlh-dbw-<suffix>`)
3. Click **Launch Workspace** — this opens the Databricks UI

## 3.2 Create a Cluster

1. In Databricks → **Compute** → **Create Cluster**
2. Configure:

| Setting            | Value                    |
| ------------------ | ------------------------ |
| Cluster Name       | `ecommerce-cluster`      |
| Cluster Mode       | Single Node              |
| Databricks Runtime | 13.3 LTS (or latest LTS) |
| Node Type          | Standard_DS3_v2          |
| Auto-terminate     | 30 minutes               |
| Spot Instances     | Enabled (60–70% savings) |

3. Click **Create Cluster**

> **Cost Tip:** The cluster auto-terminates after 30 minutes of inactivity and uses spot instances to minimize cost.

## 3.3 Grant Databricks Access to Key Vault

Back in the terminal:

```bash
source setup/variables.sh

# Get the Databricks managed identity
DATABRICKS_RESOURCE_ID=$(az databricks workspace show \
    --name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

# Grant Key Vault access
az keyvault set-policy \
    --name $KEY_VAULT_NAME \
    --object-id $(az ad signed-in-user show --query id -o tsv) \
    --secret-permissions get list
```

## 3.4 Create a Databricks Secret Scope

1. Navigate to: `https://<databricks-instance>#secrets/createScope`
2. Fill in:

| Field            | Value                                                                   |
| ---------------- | ----------------------------------------------------------------------- |
| Scope Name       | `ecommerce-scope`                                                       |
| Manage Principal | All Users                                                               |
| DNS Name         | Your Key Vault DNS (e.g. `https://ecomlh-kv-<suffix>.vault.azure.net/`) |
| Resource ID      | Key Vault Resource ID (from Portal → Key Vault → Properties)            |

3. Click **Create**

### Verify the Secret Scope

In a Databricks notebook cell:

```python
dbutils.secrets.list(scope="ecommerce-scope")
```

You should see `datalake-access-key` listed.

## 3.5 Grant Storage Permissions

The signed-in user and ADF/Synapse identities need **Storage Blob Data Contributor** on the ADLS account:

```bash
source setup/variables.sh

# Grant your own user access
USER_OBJECT_ID=$(az ad signed-in-user show --query id -o tsv)
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee "$USER_OBJECT_ID" \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
```

## 3.6 Import Notebooks

1. In Databricks → **Workspace** → **Users** → your username
2. Click **Import** → select **File**
3. Import each notebook folder:
   - `databricks/setup/mount_storage.py`
   - `databricks/bronze_to_silver/` (all 5 notebooks)
   - `databricks/silver_to_gold/` (all 5 notebooks)
   - `databricks/streaming/` (streaming notebook)
   - `databricks/utils/common_functions.py`

## 3.7 Mount Storage

Run the `mount_storage` notebook. This mounts the ADLS containers:

```
/mnt/bronze   → bronze container
/mnt/silver   → silver container
/mnt/gold     → gold container
/mnt/config   → config container
```

### Verify the Mount

```python
dbutils.fs.ls("/mnt/bronze/ecommerce/")
```

You should see the 9 CSV files uploaded in Step 2.

> **Note:** Mounts do not persist if the Databricks workspace is deleted and recreated. Re-run the mount notebook after reprovisioning.

## What's Next?

Proceed to [04_bronze_to_silver.md](04_bronze_to_silver.md) to run the data cleansing and transformation notebooks.
