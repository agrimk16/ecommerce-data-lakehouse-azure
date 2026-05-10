#!/bin/bash
# ============================================================================
# Teardown Script - Delete all Azure resources
# Run this when you're done to avoid ongoing charges
# ============================================================================
set -e

source "$(dirname "$0")/variables.sh"

echo "============================================"
echo "  WARNING: This will delete ALL resources"
echo "  in resource group: $RESOURCE_GROUP"
echo "============================================"
echo ""
echo "Resources to be deleted:"
echo "  Data Lake:      $DATALAKE_ACCOUNT"
echo "  Data Factory:   $DATA_FACTORY_NAME"
echo "  Databricks:     $DATABRICKS_WORKSPACE"
echo "  Synapse:        $SYNAPSE_WORKSPACE"
echo "  Event Hubs:     $EVENTHUB_NAMESPACE"
echo "  Key Vault:      $KEY_VAULT_NAME"
echo ""
read -p "Are you sure? (yes/no): " CONFIRM

if [ "$CONFIRM" = "yes" ]; then
    echo "Deleting resource group: $RESOURCE_GROUP..."
    az group delete \
        --name "$RESOURCE_GROUP" \
        --yes \
        --no-wait
    echo "Resource group deletion initiated."
    echo "It may take a few minutes for all resources to be removed."
    echo ""
    echo "Tip: Verify deletion in Azure Portal or run:"
    echo "  az group show --name $RESOURCE_GROUP 2>/dev/null || echo 'Deleted!'"
else
    echo "Teardown cancelled."
fi
