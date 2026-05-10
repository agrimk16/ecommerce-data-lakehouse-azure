// ============================================================
// Azure Databricks Workspace
// ============================================================

@description('Databricks workspace name')
param workspaceName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

@description('Pricing tier')
@allowed(['standard', 'premium'])
param pricingTier string = 'premium'

var managedResourceGroupName = 'databricks-rg-${workspaceName}'

resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    parameters: {
      enableNoPublicIp: {
        value: false                       // Set true for prod (private endpoint)
      }
    }
  }
}

// --- Outputs ---
output workspaceId string = databricks.id
output workspaceUrl string = 'https://${databricks.properties.workspaceUrl}'
