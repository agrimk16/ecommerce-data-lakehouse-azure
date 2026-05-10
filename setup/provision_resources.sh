#!/bin/bash
# ============================================================================
# Azure Resource Provisioning Script
# Creates all Azure resources needed for the E-Commerce Data Lakehouse
#
# IDEMPOTENT: Safe to re-run. Resources that already exist will be skipped.
# ============================================================================
set -e

# Load variables
source "$(dirname "$0")/variables.sh"

echo "============================================"
echo "  E-Commerce Data Lakehouse - Setup"
echo "============================================"
echo ""
echo "Using resources:"
echo "  Data Lake:      $DATALAKE_ACCOUNT"
echo "  Data Factory:   $DATA_FACTORY_NAME"
echo "  Databricks:     $DATABRICKS_WORKSPACE"
echo "  Synapse:        $SYNAPSE_WORKSPACE"
echo "  Event Hubs:     $EVENTHUB_NAMESPACE"
echo "  Key Vault:      $KEY_VAULT_NAME"
echo ""

# -----------------------------------------------
# 0. Register required Azure resource providers
# -----------------------------------------------
echo "[0/7] Registering Azure resource providers..."
for provider in Microsoft.Storage Microsoft.DataFactory Microsoft.Databricks Microsoft.Synapse Microsoft.EventHub Microsoft.KeyVault; do
    STATUS=$(az provider show --namespace "$provider" --query "registrationState" -o tsv 2>/dev/null || echo "NotRegistered")
    if [ "$STATUS" != "Registered" ]; then
        echo "  Registering $provider..."
        az provider register --namespace "$provider" --wait
    else
        echo "  $provider — already registered ✓"
    fi
done
echo ""

# -----------------------------------------------
# 1. Create Resource Group
# -----------------------------------------------
echo "[1/7] Creating Resource Group: $RESOURCE_GROUP"
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# -----------------------------------------------
# 2. Create Azure Data Lake Storage Gen2
# -----------------------------------------------
echo ""
echo "[2/7] Creating Data Lake Storage Gen2: $DATALAKE_ACCOUNT"
az storage account create \
    --name "$DATALAKE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# Get storage key
DL_KEY=$(az storage account keys list \
    --account-name "$DATALAKE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query "[0].value" -o tsv)

# Create containers (Bronze/Silver/Gold + Config)
echo "  Creating containers..."
for container in "$DATALAKE_CONTAINER_BRONZE" "$DATALAKE_CONTAINER_SILVER" "$DATALAKE_CONTAINER_GOLD" "$DATALAKE_CONTAINER_CONFIG"; do
    az storage container create \
        --name "$container" \
        --account-name "$DATALAKE_ACCOUNT" \
        --account-key "$DL_KEY" \
        --only-show-errors 2>/dev/null || true
done

# -----------------------------------------------
# 3. Create Azure Data Factory
# -----------------------------------------------
echo ""
echo "[3/7] Creating Azure Data Factory: $DATA_FACTORY_NAME"
az datafactory create \
    --name "$DATA_FACTORY_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# -----------------------------------------------
# 4. Create Azure Databricks Workspace
# -----------------------------------------------
echo ""
echo "[4/7] Creating Azure Databricks Workspace: $DATABRICKS_WORKSPACE"
az databricks workspace create \
    --name "$DATABRICKS_WORKSPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku premium \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# -----------------------------------------------
# 5. Create Azure Synapse Analytics Workspace
# -----------------------------------------------
echo ""
echo "[5/7] Creating Azure Synapse Workspace: $SYNAPSE_WORKSPACE"

# Synapse needs a storage account for its filesystem
SYNAPSE_DL_FS="synapsefs"
az storage container create \
    --name "$SYNAPSE_DL_FS" \
    --account-name "$DATALAKE_ACCOUNT" \
    --account-key "$DL_KEY" \
    --only-show-errors 2>/dev/null || true

az synapse workspace create \
    --name "$SYNAPSE_WORKSPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --storage-account "$DATALAKE_ACCOUNT" \
    --file-system "$SYNAPSE_DL_FS" \
    --sql-admin-login-user "$SYNAPSE_SQL_ADMIN_USER" \
    --sql-admin-login-password "$SYNAPSE_SQL_ADMIN_PASSWORD" \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# Open firewall for client IP
echo "  Opening Synapse firewall for your IP..."
CLIENT_IP=$(curl -s https://api.ipify.org)
az synapse workspace firewall-rule create \
    --name "AllowClientIP" \
    --resource-group "$RESOURCE_GROUP" \
    --workspace-name "$SYNAPSE_WORKSPACE" \
    --start-ip-address "$CLIENT_IP" \
    --end-ip-address "$CLIENT_IP" \
    --only-show-errors 2>/dev/null || true

# -----------------------------------------------
# 6. Create Azure Event Hubs Namespace + Hub
# -----------------------------------------------
echo ""
echo "[6/7] Creating Event Hubs: $EVENTHUB_NAMESPACE"
az eventhubs namespace create \
    --name "$EVENTHUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Basic \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

az eventhubs eventhub create \
    --name "$EVENTHUB_NAME" \
    --namespace-name "$EVENTHUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --partition-count 2 \
    --message-retention 1

# -----------------------------------------------
# 7. Create Azure Key Vault
# -----------------------------------------------
echo ""
echo "[7/7] Creating Key Vault: $KEY_VAULT_NAME"
az keyvault create \
    --name "$KEY_VAULT_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --enable-rbac-authorization true \
    --tags project="$TAG_PROJECT" environment="$TAG_ENVIRONMENT"

# Store Data Lake connection string as secret
DL_CONN=$(az storage account show-connection-string \
    --name "$DATALAKE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query "connectionString" -o tsv)

az keyvault secret set \
    --vault-name "$KEY_VAULT_NAME" \
    --name "datalake-connection-string" \
    --value "$DL_CONN" \
    --only-show-errors 2>/dev/null || true

az keyvault secret set \
    --vault-name "$KEY_VAULT_NAME" \
    --name "datalake-access-key" \
    --value "$DL_KEY" \
    --only-show-errors 2>/dev/null || true

# Store Event Hub connection string
EH_CONN=$(az eventhubs namespace authorization-rule keys list \
    --namespace-name "$EVENTHUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --name "RootManageSharedAccessKey" \
    --query "primaryConnectionString" -o tsv)

az keyvault secret set \
    --vault-name "$KEY_VAULT_NAME" \
    --name "eventhub-connection-string" \
    --value "$EH_CONN" \
    --only-show-errors 2>/dev/null || true

# Grant ADF Managed Identity access to Key Vault
echo "  Granting ADF managed identity access to Key Vault..."
ADF_PRINCIPAL_ID=$(az datafactory show \
    --name "$DATA_FACTORY_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "identity.principalId" -o tsv)

az role assignment create \
    --role "Key Vault Secrets User" \
    --assignee "$ADF_PRINCIPAL_ID" \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KEY_VAULT_NAME" \
    --only-show-errors 2>/dev/null || true

echo ""
echo "============================================"
echo "  ✓ All resources created successfully!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Download Kaggle dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
echo "  2. Upload CSVs to ADLS: az storage blob upload-batch --destination bronze --source data/raw/ --account-name $DATALAKE_ACCOUNT"
echo "  3. Follow the implementation guide: docs/setup/01_azure_setup.md"
echo ""
echo "Azure Portal: https://portal.azure.com/#@/resource/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP"
