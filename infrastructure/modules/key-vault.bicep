// ============================================================
// Azure Key Vault for Secrets Management
// ============================================================

@description('Key Vault name')
param keyVaultName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true         // Use RBAC instead of access policies
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    enabledForTemplateDeployment: true
    publicNetworkAccess: 'Enabled'
  }
}

// --- Outputs ---
output keyVaultName string = keyVault.name
output keyVaultUri string = keyVault.properties.vaultUri
output keyVaultId string = keyVault.id
