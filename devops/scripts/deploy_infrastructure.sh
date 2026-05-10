#!/bin/bash
# ============================================================
# Deploy Infrastructure Script
# ============================================================
# Usage: ./deploy_infrastructure.sh <environment>
# Example: ./deploy_infrastructure.sh dev
# ============================================================

set -euo pipefail

ENVIRONMENT=${1:-dev}
RESOURCE_GROUP="rg-ecommerce-lakehouse"
LOCATION="centralindia"

echo "=============================================="
echo "E-Commerce Data Lakehouse — Infrastructure"
echo "Environment: ${ENVIRONMENT}"
echo "Resource Group: ${RESOURCE_GROUP}"
echo "Location: ${LOCATION}"
echo "=============================================="

# Step 1: Create resource group
echo ""
echo "Step 1: Creating resource group..."
az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --tags project=ecommerce-data-lakehouse environment="${ENVIRONMENT}"

# Step 2: Deploy Bicep template
echo ""
echo "Step 2: Deploying infrastructure..."
az deployment group create \
  --resource-group "${RESOURCE_GROUP}" \
  --template-file ../infrastructure/main.bicep \
  --parameters "@../infrastructure/parameters/${ENVIRONMENT}.parameters.json" \
  --name "deploy-${ENVIRONMENT}-$(date +%Y%m%d%H%M)" \
  --verbose

# Step 3: Show outputs
echo ""
echo "Step 3: Deployment outputs:"
az deployment group show \
  --resource-group "${RESOURCE_GROUP}" \
  --name "$(az deployment group list --resource-group ${RESOURCE_GROUP} --query '[0].name' -o tsv)" \
  --query properties.outputs

echo ""
echo "=============================================="
echo "✓ Deployment complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Download Kaggle dataset and upload to Bronze container"
echo "  2. Configure Databricks secret scope (Key Vault backed)"
echo "  3. Run mount_storage.py notebook in Databricks"
echo "  4. Follow the creation order guide: docs/00_creation_order.md"
