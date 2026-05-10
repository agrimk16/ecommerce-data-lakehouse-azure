// ============================================================
// Azure Synapse Analytics Workspace (Serverless SQL)
// ============================================================

@description('Synapse workspace name')
param synapseName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

@description('Storage account resource ID for default data lake')
param storageAccountId string

@description('Storage account DFS endpoint URL')
param storageAccountUrl string

@description('SQL admin username')
param sqlAdminUser string

@description('SQL admin password')
@secure()
param sqlAdminPassword string

resource synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: storageAccountUrl
      filesystem: 'gold'
    }
    sqlAdministratorLogin: sqlAdminUser
    sqlAdministratorLoginPassword: sqlAdminPassword
    publicNetworkAccess: 'Enabled'
  }
}

// Allow all Azure services (for dev) — restrict in prod
resource firewallRule 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: synapse
  name: 'AllowAllAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// --- Outputs ---
output synapseWorkspaceUrl string = synapse.properties.connectivityEndpoints.web
output synapseSqlEndpoint string = synapse.properties.connectivityEndpoints.sql
