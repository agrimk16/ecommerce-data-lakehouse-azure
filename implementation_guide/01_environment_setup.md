# Step 1: Environment Setup

## 1.1 Prerequisites

| Requirement      | Notes                                        |
| ---------------- | -------------------------------------------- |
| Azure account    | Pay-As-You-Go (free trial also works)        |
| Local terminal   | Linux, macOS, or Windows WSL                 |
| Azure CLI        | `az --version` — should show 2.x.x or higher |
| Git              | `git --version`                              |
| Python 3.8+      | `python3 --version`                          |
| VS Code          | Recommended editor                           |
| Kaggle account   | Free — needed for the dataset                |
| Power BI Desktop | Free download — Windows only                 |

## 1.2 Install the Azure CLI

**Linux / WSL:**

```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az --version
```

**macOS:**

```bash
brew install azure-cli
```

## 1.3 Login and Set Subscription

```bash
az login
az account list --output table
az account set --subscription "<your-subscription-id>"
az account show --query "{Subscription:name, ID:id}" -o table
```

## 1.4 Clone the Repository

```bash
cd ~
git clone https://github.com/<your-username>/ecommerce-data-lakehouse-azure.git
cd ecommerce-data-lakehouse-azure
```

## 1.5 Configure Variables

```bash
cp setup/variables.sh.template setup/variables.sh
```

Edit `setup/variables.sh` with your values:

```bash
UNIQUE_SUFFIX="abc123"          # Your initials + 4 digits — lowercase only
SUBSCRIPTION_ID="xxxxxxxx-..."  # From: az account show --query id -o tsv
LOCATION="eastus2"              # Region closest to you
SYNAPSE_SQL_ADMIN_PASSWORD=""   # 12+ chars, mix of uppercase, lowercase, number, symbol
```

> **IMPORTANT:** Never commit `setup/variables.sh` to Git. It is already in `.gitignore`.

## 1.6 Set a Budget Alert

1. Azure Portal → search **Cost Management + Billing**
2. Click **Budgets** → **+ Add**
3. Name: `ecommerce-lakehouse-budget`, Reset: Monthly, Amount: `$30`
4. Add alert at **80%** with your email → **Create**

## 1.7 Provision All Resources

```bash
cd setup
chmod +x provision_resources.sh
./provision_resources.sh
```

This takes approximately 15 minutes and creates:

```
[1/6] Resource Group .............. rg-ecommerce-lakehouse
[2/6] ADLS Gen2 Storage ........... ecomlhdl<suffix>
[3/6] Azure Key Vault ............. ecomlh-kv-<suffix>
[4/6] Azure Databricks ............ ecomlh-dbw-<suffix>
[5/6] Azure Data Factory .......... ecomlh-adf-<suffix>
[6/6] Azure Synapse Workspace ..... ecomlh-syn-<suffix>
[7/7] Event Hubs Namespace ........ ecomlh-eh-<suffix>
```

## 1.8 Verify Resources

Go to [Azure Portal](https://portal.azure.com) → **Resource Groups** → **rg-ecommerce-lakehouse** and confirm all resources are listed.

Or via CLI:

```bash
source setup/variables.sh
az resource list --resource-group $RESOURCE_GROUP --output table
```

## 1.9 ADLS Gen2 Container Structure

The provisioning script creates these containers:

```
ecomlhdl<suffix>/
├── bronze/    ← Raw CSV files land here
├── silver/    ← Cleansed Delta tables
├── gold/      ← Star Schema fact + dimension tables
└── config/    ← Pipeline configuration files
```

## What's Next?

Proceed to [02_data_ingestion.md](02_data_ingestion.md) to download the Kaggle dataset and upload it to the Bronze layer.
