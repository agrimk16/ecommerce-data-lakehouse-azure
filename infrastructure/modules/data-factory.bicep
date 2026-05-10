// ============================================================
// Azure Data Factory with Managed Identity
// ============================================================

@description('Data Factory name')
param dataFactoryName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

@description('Key Vault name for linked service')
param keyVaultName string

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// --- Outputs ---
output dataFactoryName string = dataFactory.name
output dataFactoryId string = dataFactory.id
output principalId string = dataFactory.identity.principalId
