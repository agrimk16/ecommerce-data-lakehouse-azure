// ============================================================
// ADLS Gen2 Storage Account with Hierarchical Namespace
// ============================================================

@description('Storage account name')
param storageAccountName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true                    // Hierarchical Namespace for ADLS Gen2
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'             // Change to 'Deny' for prod
      bypass: 'AzureServices'
    }
  }
}

// --- Create containers for Medallion layers ---
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'bronze'
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'silver'
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'gold'
}

resource configContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'config'
}

// --- Outputs ---
output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
