# E-Commerce Data Lakehouse on Azure — Step-by-Step Implementation Guide

This is the single guide for building the entire project from scratch on an Azure Pay-As-You-Go subscription. Follow the steps in order. Each step includes both the **Azure Portal (UI)** instructions and, where applicable, the equivalent **CLI/script** shortcut.

---

## Table of Contents

1. [Before You Begin](#1-before-you-begin)
2. [Create Azure Resources](#2-create-azure-resources)
3. [Upload the Dataset](#3-upload-the-dataset)
4. [Configure Databricks](#4-configure-databricks)
5. [Run Bronze → Silver Transformations](#5-run-bronze--silver-transformations)
6. [Run Silver → Gold (Star Schema)](#6-run-silver--gold-star-schema)
7. [Set Up Azure Data Factory Pipelines](#7-set-up-azure-data-factory-pipelines)
8. [Set Up Synapse Analytics](#8-set-up-synapse-analytics)
9. [Real-Time Streaming (Optional)](#9-real-time-streaming-optional)
10. [Power BI Dashboard](#10-power-bi-dashboard)
11. [CI/CD with Azure DevOps (Optional)](#11-cicd-with-azure-devops-optional)
12. [Teardown — Stop Paying When Done](#12-teardown--stop-paying-when-done)

---

## 1. Before You Begin

### 1.1 What You Need

| Requirement | Notes |
|---|---|
| Azure account | Pay-As-You-Go (free trial also works) |
| Local terminal | Linux, macOS, or Windows WSL |
| Azure CLI | Installation steps below |
| Git | `git --version` to check |
| Python 3.8+ | `python3 --version` to check |
| VS Code | Recommended editor |
| Kaggle account | Free — needed for the dataset |
| Power BI Desktop | Free download — Windows only |

### 1.2 Install the Azure CLI

**Linux / WSL:**
```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az --version   # Confirm: should show 2.x.x or higher
```

**macOS:**
```bash
brew install azure-cli
```

**Windows:** Download the MSI installer from [aka.ms/installazurecliwindows](https://aka.ms/installazurecliwindows)

### 1.3 Sign in and Set Your Subscription

```bash
az login
# Your browser opens — sign in with your Azure account

az account list --output table
# Find your subscription in the list, then:
az account set --subscription "<your-subscription-id>"

az account show --query "{Subscription:name, ID:id}" -o table
# Confirm the right subscription is active
```

### 1.4 Clone the Repository

```bash
cd ~
git clone https://github.com/<your-username>/ecommerce-data-lakehouse-azure.git
cd ecommerce-data-lakehouse-azure
```

### 1.5 Configure Your Variables File

```bash
cp setup/variables.sh.template setup/variables.sh
```

Open `setup/variables.sh` in VS Code and fill in these four values:

```bash
UNIQUE_SUFFIX="abc123"          # Your initials + 4 digits, e.g. jd0042 — lowercase only, no spaces
SUBSCRIPTION_ID="xxxxxxxx-..."  # From: az account show --query id -o tsv
LOCATION="eastus2"              # Pick the region closest to you (see note below)
SYNAPSE_SQL_ADMIN_PASSWORD=""   # 12+ chars, mix of uppercase, lowercase, number, and symbol
```

> **Region Guide:** `eastus2` (US East), `westeurope` (Europe), `centralindia` (India), `australiaeast` (Australia).
> Pick the region nearest to you — it reduces latency and often costs less.

> **IMPORTANT:** Never commit `setup/variables.sh` to Git. It is already in `.gitignore`.

### 1.6 Set a Budget Alert — Do This First

This prevents unexpected charges. On Azure Portal:

1. In the search bar at the top, search for **Cost Management + Billing**
2. Click **Budgets** in the left menu → **+ Add**
3. Fill in:
   - Name: `ecommerce-lakehouse-budget`
   - Reset period: Monthly
   - Amount: `$30`
4. Click **Next** → Add an alert at **80%** → enter your email address → **Create**

---

## 2. Create Azure Resources

You need six Azure services. **Choose one option:**

- **Option A (Recommended for beginners):** Use the automated script — all six resources are created in one command.
- **Option B:** Create each resource manually in the Azure Portal.

---

### Option A — Automated Script (Recommended)

```bash
cd setup
chmod +x provision_resources.sh
./provision_resources.sh
```

This takes approximately 15 minutes. The script creates all six services in the correct order:

```
[1/6] Resource Group .............. rg-ecommerce-lakehouse
[2/6] ADLS Gen2 Storage ........... ecomlhdl<suffix>
[3/6] Azure Key Vault ............. ecomlh-kv-<suffix>
[4/6] Azure Databricks ............ ecomlh-dbw-<suffix>
[5/6] Azure Data Factory .......... ecomlh-adf-<suffix>
[6/6] Azure Synapse Workspace ..... ecomlh-syn-<suffix>
[7/7] Event Hubs Namespace ........ ecomlh-eh-<suffix>
```

When it finishes, go to the [Azure Portal](https://portal.azure.com) → **Resource Groups** → **rg-ecommerce-lakehouse** and confirm all six resources are listed.

Then **skip to [Step 3: Upload the Dataset](#3-upload-the-dataset)**.

---

### Option B — Azure Portal (Manual)

#### 2.1 Create a Resource Group

A Resource Group is a folder that holds all your Azure resources for this project.

1. Go to [portal.azure.com](https://portal.azure.com)
2. In the top search bar, type **Resource groups** and click it
3. Click **+ Create**
4. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Region | Your chosen region (e.g. East US 2) |
5. Click **Review + Create** → **Create**

#### 2.2 Create ADLS Gen2 Storage Account

This is your data lake — it holds all Bronze, Silver, and Gold data.

1. Search for **Storage accounts** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Storage account name | `ecomlhdl<your-suffix>` (e.g. `ecomlhdljd0042`) — globally unique, lowercase only |
   | Region | Same as your resource group |
   | Performance | Standard |
   | Redundancy | **Locally-redundant storage (LRS)** — cheapest option |
3. Click **Advanced** tab:
   - Enable **Hierarchical namespace** — this is what makes it ADLS Gen2 (real directories instead of flat blobs)
4. Click **Review + Create** → **Create**

**Create containers after the storage account deploys:**
1. Open the storage account → **Containers** in the left menu → **+ Container**
2. Create these four containers (each with **Private** access level):
   - `bronze`
   - `silver`
   - `gold`
   - `config`

#### 2.3 Create Azure Key Vault

Key Vault stores secrets (storage keys, passwords, API tokens) so they never appear in your code.

1. Search for **Key vaults** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Key vault name | `ecomlh-kv-<your-suffix>` — globally unique |
   | Region | Same region |
   | Pricing tier | **Standard** |
3. Click **Review + Create** → **Create**

**Store the storage access key in Key Vault:**
1. Go to your storage account → **Access keys** (left menu) → Click **Show** next to Key 1 → Copy the key
2. Go to your Key Vault → **Secrets** (left menu) → **+ Generate/Import**
3. Fill in:
   - Name: `datalake-access-key`
   - Value: Paste the storage key you copied
4. Click **Create**

#### 2.4 Create Azure Databricks Workspace

Databricks is where all PySpark data transformations run.

1. Search for **Azure Databricks** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Workspace name | `ecomlh-dbw-<your-suffix>` |
   | Region | Same region |
   | Pricing tier | **Standard** (not Premium — saves ~40% for dev work) |
3. Click **Review + Create** → **Create**

> This takes about 3–5 minutes to deploy.

#### 2.5 Create Azure Data Factory

ADF orchestrates the data pipelines — it controls when and how data moves.

1. Search for **Data factories** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Name | `ecomlh-adf-<your-suffix>` — globally unique |
   | Region | Same region |
   | Version | V2 |
3. Click **Review + Create** → **Create**

#### 2.6 Create Azure Synapse Workspace

Synapse provides a serverless SQL engine to query your Gold Delta tables directly — no dedicated cluster needed.

1. Search for **Azure Synapse Analytics** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Workspace name | `ecomlh-syn-<your-suffix>` — globally unique |
   | Region | Same region |
   | Data Lake Storage Gen2 | Select your existing ADLS account |
   | File system name | `synapse` (creates a new container) |
3. On the **Security** tab set:
   - SQL admin username: `sqladminuser`
   - SQL admin password: same strong password from your variables file
4. Click **Review + Create** → **Create**

#### 2.7 Create Event Hubs Namespace

Event Hubs is the real-time message broker for clickstream data. Only needed for the streaming phase.

1. Search for **Event Hubs** → **+ Create**
2. Fill in:
   | Field | Value |
   |---|---|
   | Subscription | Your subscription |
   | Resource group | `rg-ecommerce-lakehouse` |
   | Namespace name | `ecomlh-eh-<your-suffix>` — globally unique |
   | Region | Same region |
   | Pricing tier | **Basic** (sufficient for dev/testing) |
3. Click **Review + Create** → **Create**

**Create an Event Hub (topic) inside the namespace:**
1. Open the namespace → **Event Hubs** (left menu) → **+ Event Hub**
2. Name: `clickstream-events` → **Create**

---

## 3. Upload the Dataset

### 3.1 Download the Kaggle Dataset

**Using Kaggle CLI (recommended):**
```bash
# One-time setup: get your API token from kaggle.com → Account → API → Create New Token
mkdir -p ~/.kaggle
cp ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

pip install kaggle --quiet

# Download and extract the dataset
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/
cd data/raw && unzip -q brazilian-ecommerce.zip && cd ../..
```

**Alternative — manual download:**
1. Go to [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Click **Download** → save the ZIP → extract all `.csv` files to `data/raw/`

### 3.2 Upload CSV Files to the Bronze Container

**Using Azure Storage Explorer (UI):**

1. Download and install [Azure Storage Explorer](https://azure.microsoft.com/features/storage-explorer/) — it's free
2. Open it → Sign in with your Azure account
3. Navigate to: **Storage Accounts → your-storage-account → Blob Containers → bronze**
4. Click **New Folder** → name it `orders` → open it
5. Click **Upload → Upload Files** → select `olist_orders_dataset.csv`
6. Repeat for each dataset, creating a separate subfolder for each:

   | CSV File | Subfolder to Create |
   |---|---|
   | `olist_orders_dataset.csv` | `orders` |
   | `olist_order_items_dataset.csv` | `order_items` |
   | `olist_customers_dataset.csv` | `customers` |
   | `olist_products_dataset.csv` | `products` |
   | `olist_order_payments_dataset.csv` | `payments` |
   | `olist_sellers_dataset.csv` | `sellers` |
   | `olist_order_reviews_dataset.csv` | `reviews` |
   | `olist_geolocation_dataset.csv` | `geolocation` |

7. Also upload the config file to the `config` container:
   - Navigate to the `config` container
   - Click **Upload → Upload Files** → select `data/configs/dataset_file_list.json`
   - Upload (no subfolder needed)

**Using the CLI (alternative):**
```bash
source setup/variables.sh

DL_KEY=$(az storage account keys list \
  --account-name "$DATALAKE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[0].value" -o tsv)

for entry in "olist_orders_dataset.csv:orders" \
             "olist_order_items_dataset.csv:order_items" \
             "olist_customers_dataset.csv:customers" \
             "olist_products_dataset.csv:products" \
             "olist_order_payments_dataset.csv:payments" \
             "olist_sellers_dataset.csv:sellers" \
             "olist_order_reviews_dataset.csv:reviews" \
             "olist_geolocation_dataset.csv:geolocation"; do
  file="${entry%%:*}"
  folder="${entry##*:}"
  az storage blob upload \
    --account-name "$DATALAKE_ACCOUNT" --account-key "$DL_KEY" \
    --container-name bronze --file "data/raw/$file" \
    --name "$folder/$file" --only-show-errors
  echo "Uploaded $file → bronze/$folder/"
done

az storage blob upload \
  --account-name "$DATALAKE_ACCOUNT" --account-key "$DL_KEY" \
  --container-name config --file "data/configs/dataset_file_list.json" \
  --name "dataset_file_list.json"
echo "All files uploaded."
```

---

## 4. Configure Databricks

### 4.1 Launch the Databricks Workspace

1. Go to [portal.azure.com](https://portal.azure.com)
2. Open **Resource Groups → rg-ecommerce-lakehouse**
3. Click your Databricks workspace (`ecomlh-dbw-...`)
4. Click **Launch Workspace** — this opens Databricks in a new browser tab

### 4.2 Create a Compute Cluster

A cluster is the computing engine that runs your PySpark notebooks.

1. In Databricks, click **Compute** in the left sidebar
2. Click **Create compute**
3. Configure as follows:

   | Setting | Value | Why |
   |---|---|---|
   | Cluster name | `ecommerce-dev-cluster` | Identifiable name |
   | Policy | Unrestricted | Gives full control |
   | Single node | ✅ Enable (tick this checkbox) | Cheapest for dev — no worker nodes |
   | Databricks Runtime | `14.3 LTS (Spark 3.5, Scala 2.12)` | Stable long-term support version |
   | Node type | `Standard_DS3_v2` | 4 CPU, 14 GB RAM — sufficient and cheap |
   | Terminate after | `30` minutes of inactivity | **Critical for cost** — auto-stops when idle |
   | Spot instances | ✅ Enable | Up to 70% cheaper than on-demand |

4. Click **Create compute**

> The cluster starts in about 3 minutes. The status dot turns green when ready.

**Cost tip:** The cluster only costs money when it's running. With 30-minute auto-terminate and spot pricing, a full day of intermittent work costs under $3.

### 4.3 Create a Key Vault-Backed Secret Scope

This is how notebooks securely read your storage key and other secrets without hardcoding them.

1. While your Databricks workspace is open in the browser, modify the URL:
   - Take your current URL: `https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net/...`
   - Replace the path with `#secrets/createScope`
   - Full URL: `https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net/#secrets/createScope`
   - Press Enter

2. Fill in the form:
   | Field | Value |
   |---|---|
   | Scope Name | `ecommerce-kv-scope` |
   | Manage Principal | All Users |
   | DNS Name | `https://ecomlh-kv-<your-suffix>.vault.azure.net/` |
   | Resource ID | (see below) |

3. Get the Resource ID for the Resource ID field:
   ```bash
   source setup/variables.sh
   az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" --query "id" -o tsv
   ```
   Copy the output (it looks like `/subscriptions/xxx/resourceGroups/rg-.../providers/Microsoft.KeyVault/vaults/...`) and paste it into the Resource ID field.

4. Click **Create**

### 4.4 Grant Permissions

Databricks needs to read from Key Vault and write to storage.

**Grant access to Key Vault:**
```bash
source setup/variables.sh

# Get the Databricks application's Object ID
DBKS_APP_ID=$(az ad sp list --display-name "AzureDatabricks" --query "[0].id" -o tsv)

az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "$DBKS_APP_ID" \
  --scope "$(az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
```

**Also grant your own user access** (so you can view secrets for debugging):
```bash
MY_OID=$(az ad signed-in-user show --query id -o tsv)
az role assignment create \
  --role "Key Vault Administrator" \
  --assignee "$MY_OID" \
  --scope "$(az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
```

### 4.5 Import Notebooks into Databricks

1. In Databricks, click **Workspace** in the left sidebar
2. Click the dropdown arrow next to your username → **Create → Folder**
3. Name the folder: `ecommerce-lakehouse`
4. Inside that folder, create these five subfolders (same process):
   - `setup`
   - `utils`
   - `bronze_to_silver`
   - `silver_to_gold`
   - `streaming`
5. To import notebooks into each folder:
   - Click into the target folder (e.g. `bronze_to_silver`)
   - Click the **Import** icon (downward arrow) → **File**
   - Browse to the corresponding `.py` file from your local project
   - Click **Import**

   Import each file to its matching folder:

   | Local File | Databricks Folder |
   |---|---|
   | `databricks/setup/mount_storage.py` | `setup` |
   | `databricks/utils/common_functions.py` | `utils` |
   | `databricks/bronze_to_silver/01_orders_bronze_to_silver.py` | `bronze_to_silver` |
   | `databricks/bronze_to_silver/02_customers_bronze_to_silver.py` | `bronze_to_silver` |
   | `databricks/bronze_to_silver/03_products_bronze_to_silver.py` | `bronze_to_silver` |
   | `databricks/bronze_to_silver/04_payments_bronze_to_silver.py` | `bronze_to_silver` |
   | `databricks/bronze_to_silver/05_order_items_bronze_to_silver.py` | `bronze_to_silver` |
   | `databricks/silver_to_gold/01_fact_orders.py` | `silver_to_gold` |
   | `databricks/silver_to_gold/02_dim_customers.py` | `silver_to_gold` |
   | `databricks/silver_to_gold/03_dim_products.py` | `silver_to_gold` |
   | `databricks/silver_to_gold/04_dim_date.py` | `silver_to_gold` |
   | `databricks/silver_to_gold/05_dim_geography.py` | `silver_to_gold` |
   | `databricks/streaming/stream_clickstream_events.py` | `streaming` |

### 4.6 Mount ADLS Gen2 Storage

This gives notebooks access to your data lake using simple paths like `/mnt/bronze/`.

1. In Databricks, open the notebook: **Workspace → ecommerce-lakehouse → setup → mount_storage**
2. In the first code cell, update the storage account name:
   ```python
   storage_account_name = "ecomlhdl<your-suffix>"  # Replace with your actual name
   ```
3. Make sure your cluster is selected in the top-right dropdown (it shows the cluster name)
4. Click **Run All** (at the top of the notebook)

Expected output:
```
Mounting ADLS Gen2 containers...
  ✓ Mounted bronze  →  /mnt/bronze
  ✓ Mounted silver  →  /mnt/silver
  ✓ Mounted gold    →  /mnt/gold
  ✓ Mounted config  →  /mnt/config
All mounts complete!
```

**Verify the data is accessible:**
```python
# Add a new cell at the bottom and run it
display(dbutils.fs.ls("/mnt/bronze/"))
# You should see: orders/, customers/, products/, etc.
```

---

## 5. Run Bronze → Silver Transformations

These notebooks clean and validate the raw CSV data, then write it as Delta Lake tables. Run them in this exact order.

For each notebook:
1. Open it in Databricks (Workspace → ecommerce-lakehouse → bronze_to_silver)
2. Confirm the cluster is attached (top-right)
3. Click **Run All**
4. Wait for all cells to complete (green checkmarks)
5. Check the final cell's output for record counts

### Execution Order

| Order | Notebook | What It Does | Expected Output |
|---|---|---|---|
| **1** | `01_orders_bronze_to_silver` | Casts timestamps, validates status values, calculates delivery days | ~99,000 records |
| **2** | `02_customers_bronze_to_silver` | Cleans city/state strings, standardises capitalisation | ~99,000 records |
| **3** | `03_products_bronze_to_silver` | Fills missing categories, translates to English, removes invalid sizes | ~32,000 records |
| **4** | `04_payments_bronze_to_silver` | Groups payments by order, validates amounts | ~99,000 records |
| **5** | `05_order_items_bronze_to_silver` | Validates prices, rounds currency values, deduplicates by order+item | ~113,000 records |

### Verify All Silver Tables Exist

After all five notebooks complete, open a new notebook and run:

```python
for table in ["orders", "customers", "products", "payment_details", "payment_summary", "order_items"]:
    try:
        count = spark.read.format("delta").load(f"/mnt/silver/{table}").count()
        print(f"  silver/{table}: {count:,} records")
    except Exception as e:
        print(f"  MISSING: silver/{table} — {e}")
```

All six tables should return record counts.

---

## 6. Run Silver → Gold (Star Schema)

These notebooks build the final analytical model — a Star Schema with one fact table and four dimension tables.

**Run in this exact order** — dimensions must exist before the fact table:

| Order | Notebook | Table Created | Notes |
|---|---|---|---|
| **1** | `04_dim_date` | `gold/dim_date` | Generates all dates 2016–2026 |
| **2** | `03_dim_products` | `gold/dim_products` | Product attributes with category mapping |
| **3** | `05_dim_geography` | `gold/dim_geography` | ZIP codes with state/region lookup |
| **4** | `02_dim_customers` | `gold/dim_customers` | Customer history with SCD Type 2 |
| **5** | `01_fact_orders` | `gold/fact_orders` | Central fact table — run last |

Open each notebook (Workspace → ecommerce-lakehouse → silver_to_gold), attach your cluster, click **Run All**,  and wait for it to complete before starting the next one.

### Verify the Star Schema

```python
tables = ["dim_date", "dim_products", "dim_geography", "dim_customers", "fact_orders"]
for t in tables:
    count = spark.read.format("delta").load(f"/mnt/gold/{t}").count()
    print(f"  gold/{t}: {count:,} records")

# Confirm referential integrity: every fact row must join to dim_date
from pyspark.sql.functions import col
df_fact = spark.read.format("delta").load("/mnt/gold/fact_orders")
df_date = spark.read.format("delta").load("/mnt/gold/dim_date")
orphans = df_fact.join(df_date, df_fact.order_date_key == df_date.date_key, "left") \
                 .filter(df_date.date_key.isNull()).count()
print(f"\nOrphan fact rows (should be 0): {orphans}")
```

---

## 7. Set Up Azure Data Factory Pipelines

ADF orchestrates the entire pipeline end-to-end — ingestion, transformation, and scheduling.

### 7.1 Grant ADF Access to Resources

Before opening ADF Studio, ADF's managed identity needs permissions:

```bash
source setup/variables.sh

# Get ADF's managed identity
ADF_OID=$(az datafactory show \
  --name "$DATA_FACTORY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "identity.principalId" -o tsv)

# Storage access
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee "$ADF_OID" \
  --scope "$(az storage account show --name "$DATALAKE_ACCOUNT" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"

# Key Vault access
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "$ADF_OID" \
  --scope "$(az keyvault show --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
```

### 7.2 Open ADF Studio

1. Go to the Azure Portal → your Data Factory resource
2. Click **Launch Studio**

### 7.3 Create Linked Services

Linked Services are connections to external systems. Create them in this order.

**Go to: Manage (toolbox icon) → Linked services → + New**

#### Key Vault Linked Service
| Field | Value |
|---|---|
| Name | `ls_akv_ecommerce_keyvault` |
| Type | Azure Key Vault |
| Authentication method | System Assigned Managed Identity |
| Azure Key Vault URL | Select your Key Vault from the dropdown |

Click **Test connection** — it should say "Connection successful". Click **Create**.

#### ADLS Gen2 Linked Service
| Field | Value |
|---|---|
| Name | `ls_adls_ecommerce_datalake` |
| Type | Azure Data Lake Storage Gen2 |
| Authentication method | System Assigned Managed Identity |
| Storage account | Select your storage account from the dropdown |

Click **Test connection** → **Create**.

#### Databricks Linked Service

First, create a Databricks Personal Access Token:
- In Databricks: Click your profile icon (top right) → **Settings → Developer → Access tokens → Generate new token**
- Description: `adf-connection` → **Generate** → **Copy the token immediately** (you cannot see it again)

Store it in Key Vault:
```bash
source setup/variables.sh
az keyvault secret set \
  --vault-name "$KEY_VAULT_NAME" \
  --name "databricks-access-token" \
  --value "<paste-your-token-here>"
```

Now in ADF, create the Databricks linked service:
| Field | Value |
|---|---|
| Name | `ls_databricks_ecommerce` |
| Type | Azure Databricks |
| Databricks workspace | Select from dropdown |
| Cluster | Existing interactive cluster |
| Access token | Azure Key Vault → Secret name: `databricks-access-token` |
| Existing cluster ID | (see below) |

Get the cluster ID from Databricks: Open your cluster → look at the URL — it contains `clusters/<cluster-id>`. Copy just the cluster ID.

Click **Test connection** → **Create**.

### 7.4 Create Datasets

**Go to: Author (pencil icon) → Datasets → ... → New dataset**

Create these three datasets:

#### Dataset 1 — Config File List (JSON)
| Field | Value |
|---|---|
| Name | `ds_ecommerce_file_list` |
| Type | JSON |
| Linked service | `ls_adls_ecommerce_datalake` |
| File path | Container: `config` / File: `dataset_file_list.json` |

#### Dataset 2 — Bronze Parquet (parameterised)
| Field | Value |
|---|---|
| Name | `ds_ecommerce_bronze_parquet` |
| Type | Parquet |
| Linked service | `ls_adls_ecommerce_datalake` |
| File path | Container: `bronze` / Folder: `@dataset().folderName` (add parameter `folderName`) |

#### Dataset 3 — Silver Delta (parameterised)
| Field | Value |
|---|---|
| Name | `ds_ecommerce_silver_delta` |
| Type | Parquet |
| Linked service | `ls_adls_ecommerce_datalake` |
| File path | Container: `silver` / Folder: `@dataset().tableName` (add parameter `tableName`) |

### 7.5 Create Pipelines

**Go to: Author → Pipelines → ... → New pipeline**

#### Pipeline 1 — `pl_transform_bronze_to_silver`

Add five **Databricks Notebook** activities, one for each notebook. For each activity:
- Drag **Databricks → Notebook** onto the canvas
- Settings:
  - Linked service: `ls_databricks_ecommerce`
  - Notebook path: e.g. `/Users/<your-email>/ecommerce-lakehouse/bronze_to_silver/01_orders_bronze_to_silver`
  - Timeout: 2 hours
  - Retries: 1

Connect the activities: `orders → customers → products → payments → order_items` (each depends on the previous).

Click **Debug** to test. Watch each activity turn green.

#### Pipeline 2 — `pl_transform_silver_to_gold`

Same pattern — five Databricks Notebook activities in this order:
`dim_date → dim_products → dim_geography → dim_customers → fact_orders`

#### Pipeline 3 — `pl_master_pipeline`

This is the top-level orchestrator. Use **Execute Pipeline** activities (not Notebook activities):
1. Add **Execute Pipeline** → Settings → Invoked pipeline: `pl_transform_bronze_to_silver` → label it "Bronze to Silver"
2. Add a second **Execute Pipeline** → `pl_transform_silver_to_gold` → label it "Silver to Gold"
3. Connect them: "Silver to Gold" depends on "Bronze to Silver"

Click **Debug** to run the full end-to-end pipeline.

### 7.6 Add a Schedule Trigger (Optional)

To run the pipeline automatically every day:

1. Open `pl_master_pipeline` → click **Add trigger → New/Edit**
2. Click **+ New** → fill in:
   | Field | Value |
   |---|---|
   | Name | `tr_daily_ecommerce_schedule` |
   | Type | Schedule |
   | Start date | Tomorrow at 02:00 AM |
   | Recurrence | Every 1 Day |
3. Click **OK** → **Save all** → **Publish all**

---

## 8. Set Up Synapse Analytics

Synapse provides a SQL interface to query your Gold Delta tables directly without any data movement.

### 8.1 Grant Synapse Access to Storage

```bash
source setup/variables.sh

SYNAPSE_OID=$(az synapse workspace show \
  --name "$SYNAPSE_WORKSPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --query "identity.principalId" -o tsv)

az role assignment create \
  --role "Storage Blob Data Reader" \
  --assignee "$SYNAPSE_OID" \
  --scope "$(az storage account show --name "$DATALAKE_ACCOUNT" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
```

### 8.2 Open Synapse Studio

1. Azure Portal → your Synapse workspace
2. Click **Open Synapse Studio**

### 8.3 Run the Setup Scripts

In Synapse Studio, go to **Develop → + → SQL script**.

**Step 1: Create external tables and data sources**

Open `synapse/external_tables/create_gold_external_tables.sql` from your local project.

Before running, replace these two placeholders in the script:
- `<your-master-key-password>` → any strong password (this is a database-level encryption key, not your Azure password)
- `<storage-account-name>` → your actual storage account name (e.g. `ecomlhdljd0042`)

Paste the full script into the SQL editor, make the replacements, then click **Run**.

Expected result: `Commands completed successfully.`

**Step 2: Create the analytical views**

Run each of these files (copy each into a new SQL script, replace the storage account name if present, run):
- `synapse/views/vw_daily_revenue.sql`
- `synapse/views/vw_customer_lifetime_value.sql`
- `synapse/views/vw_sales_by_category.sql`
- `synapse/views/vw_seller_performance.sql`

**Step 3: Run the stored procedure**

Run `synapse/stored_procedures/sp_refresh_materialized_views.sql`.

### 8.4 Query Your Data

Try these queries in Synapse Studio to confirm everything works:

```sql
USE ecommerce_gold;
GO

-- Monthly revenue trend
SELECT
    d.year_month,
    COUNT(DISTINCT f.order_id)   AS total_orders,
    ROUND(SUM(f.total_payment_value), 2) AS revenue
FROM fact_orders f
JOIN dim_date d ON f.order_date_key = d.date_key
WHERE f.order_status = 'DELIVERED'
GROUP BY d.year_month
ORDER BY d.year_month;

-- Revenue by product category
SELECT TOP 10 * FROM vw_sales_by_category ORDER BY total_revenue DESC;

-- Top customers by lifetime value
SELECT TOP 10 * FROM vw_customer_lifetime_value ORDER BY lifetime_value DESC;

-- Daily revenue (last 30 days)
SELECT * FROM vw_daily_revenue ORDER BY order_date DESC;
```

---

## 9. Real-Time Streaming (Optional)

This section sets up a live clickstream feed using Event Hubs and processes it with Spark Structured Streaming. It is optional — skip it if you only want the batch pipeline.

### 9.1 Get the Event Hubs Connection String

```bash
source setup/variables.sh

EH_CONN=$(az eventhubs namespace authorization-rule keys list \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --name "RootManageSharedAccessKey" \
  --query "primaryConnectionString" -o tsv)

# Store it in Key Vault for the streaming notebook to use
az keyvault secret set \
  --vault-name "$KEY_VAULT_NAME" \
  --name "eventhub-connection-string" \
  --value "$EH_CONN"

echo "Stored in Key Vault as: eventhub-connection-string"
```

### 9.2 Install the Event Hubs Library on Your Databricks Cluster

1. In Databricks, click **Compute** → your cluster → **Libraries** tab
2. Click **Install new** → Source: **Maven**
3. Coordinates: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
4. Click **Install** — wait for status to show "Installed"

### 9.3 Start the Streaming Notebook

1. Open Databricks: Workspace → ecommerce-lakehouse → streaming → `stream_clickstream_events`
2. Run cells one by one (Shift+Enter) until you reach the final streaming cell
3. The final cell starts a continuous streaming query — leave it running

### 9.4 Send Simulated Clickstream Events

Open a new terminal on your local machine:

```bash
cd /home/coder/ecommerce-data-lakehouse-azure
pip install azure-eventhub faker --quiet

# Pass the connection string (get it from Key Vault)
source setup/variables.sh
export EVENT_HUB_CONNECTION_STRING=$(az keyvault secret show \
  --vault-name "$KEY_VAULT_NAME" --name "eventhub-connection-string" \
  --query "value" -o tsv)

python data/simulated/generate_clickstream.py
# Sends ~5 events/second for 5 minutes
```

In your Databricks streaming notebook, you should see the event counter incrementing.

### 9.5 Stop and Clean Up

After testing:
1. In the Databricks streaming notebook, run: `spark.streams.active[0].stop()`
2. Delete the Event Hubs namespace — it costs ~$0.75/day even with no traffic:
   ```bash
   az eventhubs namespace delete \
     --name "$EVENTHUB_NAMESPACE" \
     --resource-group "$RESOURCE_GROUP"
   ```

---

## 10. Power BI Dashboard

### 10.1 Install Power BI Desktop

Download for free from [powerbi.microsoft.com/downloads](https://powerbi.microsoft.com/downloads/). Windows only.

### 10.2 Get Your Synapse SQL Endpoint

```bash
source setup/variables.sh
az synapse workspace show \
  --name "$SYNAPSE_WORKSPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --query "connectivityEndpoints.sql" -o tsv
# Output: <workspace-name>.sql.azuresynapse.net
```

### 10.3 Connect Power BI to Synapse

1. Open Power BI Desktop → **Get Data** → **Azure → Azure Synapse Analytics SQL**
2. Server: paste the endpoint from above (e.g. `ecomlh-syn-abc123.sql.azuresynapse.net`)
3. Database: `ecommerce_gold`
4. Data Connectivity mode: **DirectQuery** — this queries live data without importing it
5. Click **OK** → Sign in with your Azure account when prompted

### 10.4 Build the Dashboard

In the Fields pane on the right, you will see your views and tables. Build four visuals:

| Visual | Source | X-Axis / Legend | Y-Axis / Value |
|---|---|---|---|
| Daily Revenue (line chart) | `vw_daily_revenue` | `order_date` | `daily_revenue` |
| Revenue by Category (bar chart) | `vw_sales_by_category` | `product_category` | `total_revenue` |
| Customer Segments (donut chart) | `vw_customer_lifetime_value` | `value_segment` | Count of customers |
| Orders by State (map) | `vw_seller_performance` | `seller_state` | `total_orders` |

---

## 11. CI/CD with Azure DevOps (Optional)

### 11.1 Create an Azure DevOps Project

1. Go to [dev.azure.com](https://dev.azure.com) → sign in
2. Click **+ New organization** (or use existing) → **+ New project**
3. Name: `ecommerce-lakehouse` → **Create**

### 11.2 Create a Variable Group

1. Pipelines → **Library** → **+ Variable group**
2. Name: `ecommerce-lakehouse-vars`
3. Add variables:
   | Variable | Value | Secret? |
   |---|---|---|
   | `synapseSqlAdminUser` | `sqladminuser` | No |
   | `synapseSqlPassword` | your SQL password | **Yes (lock icon)** |
   | `databricksWorkspaceUrl` | your Databricks URL | No |
   | `storageAccountName` | your storage account name | No |
4. Click **Save**

### 11.3 Create the Pipeline

1. Pipelines → **+ Create Pipeline**
2. **GitHub** → Authorise → select your repository
3. **Existing Azure Pipelines YAML file** → Branch: `main` → Path: `/devops/azure-pipelines.yml`
4. Click **Continue** → **Save and run**

The pipeline runs in five stages: Validate → Deploy Infrastructure → Deploy ADF → Deploy Databricks → Deploy Synapse.

---

## 12. Teardown — Stop Paying When Done

### At the End of Every Session

Run the teardown script:
```bash
cd setup
chmod +x teardown_resources.sh
./teardown_resources.sh
```

When prompted, type `yes` to confirm. This deletes all compute resources (Databricks clusters, Synapse pools) but **preserves your storage** so your data is still there next session.

### Resuming Next Session

When you come back:
```bash
./provision_resources.sh
```

This recreates the compute resources. Your data in Bronze, Silver, and Gold is still intact because the storage account is preserved.

The only step to repeat is mounting storage in Databricks (Section 4.6 — mounts do not persist across workspace recreation).

### Full Project Teardown (When Completely Done)

To delete everything, including the storage data:
```bash
az group delete --name rg-ecommerce-lakehouse --yes
```

> This is irreversible. Only do this when entirely finished with the project.

---

## Cost Optimisation Reference

| Practice | Savings | How |
|---|---|---|
| Databricks spot instances | 60–70% | Enable during cluster creation (Section 4.2) |
| Databricks auto-terminate (30 min) | Prevents idle waste | Set during cluster creation |
| Standard Databricks tier (not Premium) | ~40% | Already set in `infrastructure/parameters/dev.parameters.json` |
| Synapse Serverless SQL only | No idle cost | Do not create a Dedicated SQL Pool |
| Delete Event Hubs after testing | ~$22/month saving | See Section 9.5 |
| LRS storage redundancy | 40% vs GRS | Already set — sufficient for dev |
| Teardown daily | Largest saving | Run `teardown_resources.sh` each session |
| Budget alert at $30 | Safety net | Set up in Section 1.6 |

---

## Troubleshooting

**Cluster not starting:**
Check for quota limits: Azure Portal → Subscriptions → your subscription → Usage + quotas. Search for `Standard DSv2`. Request a quota increase if you see 0 available.

**Secret scope creation fails:**
Ensure your role is at least Contributor on the Key Vault. Run the grant command in Section 4.4 again.

**`/mnt/bronze/` shows as empty after mounting:**
Verify that CSV files were uploaded to the correct container paths (Section 3.2).

**Synapse query fails with "External table not found":**
Re-run `create_gold_external_tables.sql` with the correct storage account name (Section 8.3).

**ADF pipeline activity fails:**
Click the failed activity → click the error details icon → read the error message. The most common cause is a missing permission — re-run the `az role assignment create` commands in Section 7.1.

**Power BI cannot connect:**
Ensure your Azure account has been granted the `Synapse SQL Administrator` role on the Synapse workspace: Azure Portal → Synapse workspace → Access Control (IAM) → Add role assignment.
