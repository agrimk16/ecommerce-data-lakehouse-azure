// ============================================================
// Main Bicep Template — E-Commerce Data Lakehouse Infrastructure
// ============================================================
// Deploys all Azure resources needed for the project
// Usage:
//   az deployment group create \
//     --resource-group rg-ecommerce-lakehouse \
//     --template-file main.bicep \
//     --parameters @parameters/dev.parameters.json
// ============================================================

@description('Environment name (dev, prod)')
@allowed(['dev', 'prod'])
param environment string = 'dev'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Project prefix for naming')
param projectPrefix string = 'ecomlh'

@description('Databricks pricing tier')
@allowed(['standard', 'premium'])
param databricksTier string = 'premium'

@description('Synapse SQL admin username')
param synapseSqlAdminUser string = 'sqladminuser'

@description('Synapse SQL admin password')
@secure()
param synapseSqlAdminPassword string

// --- Unique naming ---
var uniqueSuffix = uniqueString(resourceGroup().id)
var storageAccountName = '${projectPrefix}adls${uniqueSuffix}'
var dataFactoryName = '${projectPrefix}-adf-${environment}'
var databricksName = '${projectPrefix}-dbw-${environment}'
var synapseName = '${projectPrefix}-syn-${environment}'
var eventHubNamespaceName = '${projectPrefix}-ehns-${environment}'
var keyVaultName = '${projectPrefix}-kv-${uniqueSuffix}'

// --- Tags ---
var commonTags = {
  project: 'ecommerce-data-lakehouse'
  environment: environment
  managedBy: 'bicep'
}

// ============================================================
// Module Deployments
// ============================================================

module storageAccount 'modules/storage-account.bicep' = {
  name: 'deploy-storage'
  params: {
    storageAccountName: storageAccountName
    location: location
    tags: commonTags
  }
}

module dataFactory 'modules/data-factory.bicep' = {
  name: 'deploy-adf'
  params: {
    dataFactoryName: dataFactoryName
    location: location
    tags: commonTags
    keyVaultName: keyVault.outputs.keyVaultName
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'deploy-databricks'
  params: {
    workspaceName: databricksName
    location: location
    tags: commonTags
    pricingTier: databricksTier
  }
}

module synapse 'modules/synapse.bicep' = {
  name: 'deploy-synapse'
  params: {
    synapseName: synapseName
    location: location
    tags: commonTags
    storageAccountId: storageAccount.outputs.storageAccountId
    storageAccountUrl: storageAccount.outputs.dfsEndpoint
    sqlAdminUser: synapseSqlAdminUser
    sqlAdminPassword: synapseSqlAdminPassword
  }
}

module eventHub 'modules/event-hub.bicep' = {
  name: 'deploy-eventhub'
  params: {
    namespaceName: eventHubNamespaceName
    location: location
    tags: commonTags
  }
}

module keyVault 'modules/key-vault.bicep' = {
  name: 'deploy-keyvault'
  params: {
    keyVaultName: keyVaultName
    location: location
    tags: commonTags
  }
}

// ============================================================
// Outputs
// ============================================================

output storageAccountName string = storageAccount.outputs.storageAccountName
output dataFactoryName string = dataFactory.outputs.dataFactoryName
output databricksWorkspaceUrl string = databricks.outputs.workspaceUrl
output synapseWorkspaceUrl string = synapse.outputs.synapseWorkspaceUrl
output eventHubNamespace string = eventHub.outputs.namespaceName
output keyVaultUri string = keyVault.outputs.keyVaultUri
