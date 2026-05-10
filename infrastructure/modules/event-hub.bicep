// ============================================================
// Azure Event Hubs Namespace + Hub for Clickstream Streaming
// ============================================================

@description('Event Hub Namespace name')
param namespaceName string

@description('Azure region')
param location string

@description('Resource tags')
param tags object

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2022-10-01-preview' = {
  name: namespaceName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
    tier: 'Basic'
    capacity: 1
  }
}

resource clickstreamHub 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  parent: eventHubNamespace
  name: 'clickstream-events'
  properties: {
    messageRetentionInDays: 1
    partitionCount: 2
  }
}

// Consumer group for Databricks Spark Streaming
resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2022-10-01-preview' = {
  parent: clickstreamHub
  name: 'databricks-cg'
}

// --- Outputs ---
output namespaceName string = eventHubNamespace.name
output eventHubName string = clickstreamHub.name
