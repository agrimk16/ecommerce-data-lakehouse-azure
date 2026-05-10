# Step 8: Real-Time Streaming & CI/CD

## Part A: Event Hubs Streaming

### Concepts

Azure Event Hubs ingests real-time clickstream events that Spark Structured Streaming processes into the Bronze layer. This demonstrates a Lambda Architecture extension to the batch Medallion pipeline.

**Key Interview Concepts:**

- **Spark Structured Streaming**: Micro-batch processing with exactly-once guarantees via checkpointing
- **Event Hubs + Kafka Protocol**: Event Hubs exposes a Kafka-compatible endpoint — Spark reads it like a Kafka topic
- **Checkpointing**: Streaming state saved to ADLS — enables recovery without data loss or duplication
- **Watermarking**: Handles late-arriving events by defining how long to wait before closing a window

### 8.1 Event Hubs Connection

The Event Hubs namespace was provisioned in Step 1. Retrieve the connection string:

```bash
source setup/variables.sh

# Get connection string
az eventhubs namespace authorization-rule keys list \
    --resource-group $RESOURCE_GROUP \
    --namespace-name $EVENTHUB_NAMESPACE \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString -o tsv
```

Store it in Key Vault:

```bash
az keyvault secret set \
    --vault-name $KEY_VAULT_NAME \
    --name "eventhub-connection-string" \
    --value "<connection-string-from-above>"
```

### 8.2 Install Maven Library on Databricks

1. Databricks → **Compute** → your cluster → **Libraries** → **Install New**
2. Select **Maven** and enter:
   - Coordinates: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
3. Click **Install** and restart the cluster

### 8.3 Run the Streaming Notebook

Open `databricks/streaming/stream_clickstream.py` and run it. The notebook:

1. Reads the Event Hub connection string from Key Vault via the secret scope
2. Connects to the Event Hub using Spark Structured Streaming
3. Parses JSON events (user_id, product_id, event_type, timestamp)
4. Writes to `/mnt/bronze/clickstream/` in Delta format with checkpointing

```python
# Core streaming logic (simplified)
stream_df = (spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
    .select(from_json(col("body").cast("string"), schema).alias("data"))
    .select("data.*")
)

stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/bronze/clickstream/_checkpoint") \
    .start("/mnt/bronze/clickstream/")
```

### 8.4 Simulate Events

In a separate terminal, run the Python event simulator:

```bash
cd data/simulated/
python simulate_clickstream.py --events 1000 --rate 50
```

This sends 1,000 simulated clickstream events to Event Hubs at 50 events/second.

### 8.5 Verify Streaming Output

In Databricks:

```python
df = spark.read.format("delta").load("/mnt/bronze/clickstream/")
df.show(5)
df.count()
```

> **Cost Tip:** Stop the streaming query and delete Event Hubs when not testing (~$22/month saving). The batch pipeline is unaffected.

---

## Part B: CI/CD with Azure DevOps

### Concepts

Azure DevOps automates testing, validation, and deployment of the data platform. The YAML pipeline runs pytest on every pull request and deploys infrastructure changes on merge to main.

**Key Interview Concepts:**

- **YAML Pipelines**: Infrastructure-as-code for CI/CD — version-controlled, reviewable, repeatable
- **Variable Groups**: Centralized secrets and config — linked to Key Vault for security
- **Multi-Stage Pipelines**: Separate stages for build (test) and deploy (release)
- **Automated Testing**: pytest validates data layer contracts on every code change

### 8.6 Azure DevOps Setup

1. Go to [dev.azure.com](https://dev.azure.com) → **New Project** → name: `ecommerce-data-lakehouse`
2. Push your repo: **Repos** → **Import** or push via Git

### 8.7 Create a Variable Group

1. **Pipelines** → **Library** → **+ Variable Group**
2. Name: `ecommerce-lakehouse-vars`
3. Toggle **Link secrets from an Azure key vault**
4. Select your Key Vault and authorize
5. Add these variables: `datalake-access-key`, `eventhub-connection-string`

### 8.8 Configure the Pipeline

The YAML pipeline is at `devops/azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main

stages:
  - stage: Test
    jobs:
      - job: RunTests
        pool:
          vmImage: "ubuntu-latest"
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.9"
          - script: |
              pip install -r requirements.txt
              pytest tests/ -v --junitxml=test-results.xml
            displayName: "Run pytest"
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: "test-results.xml"

  - stage: Deploy
    dependsOn: Test
    condition: succeeded()
    jobs:
      - job: DeployInfra
        steps:
          - task: AzureCLI@2
            inputs:
              azureSubscription: "ecommerce-service-connection"
              scriptType: "bash"
              scriptLocation: "inlineScript"
              inlineScript: |
                az deployment group create \
                  --resource-group rg-ecommerce-lakehouse \
                  --template-file infrastructure/main.bicep \
                  --parameters infrastructure/parameters/dev.parameters.json
```

### 8.9 Create the Pipeline in DevOps

1. **Pipelines** → **New Pipeline** → **Azure Repos Git** → select your repo
2. Select **Existing Azure Pipelines YAML file** → path: `devops/azure-pipelines.yml`
3. Click **Run**

---

## Part C: Testing

### 8.10 pytest Suite

The project includes three test files in `tests/`:

| Test File              | What It Tests                                     |
| ---------------------- | ------------------------------------------------- |
| `test_bronze_layer.py` | Raw data schema, row counts, null thresholds      |
| `test_silver_layer.py` | Cleansing logic, type casting, dedup verification |
| `test_gold_layer.py`   | Star schema integrity, fact-dimension joins, SCD2 |

Run locally:

```bash
pip install pytest pyspark delta-spark
pytest tests/ -v
```

**Interview Talking Point:** "Each layer has its own test suite. Bronze tests verify raw data contracts. Silver tests validate cleansing rules. Gold tests ensure referential integrity between fact and dimension tables and verify SCD Type 2 history tracking."

---

## Teardown

### Partial Teardown (Keep Data, Stop Paying for Compute)

```bash
bash setup/teardown_resources.sh
```

This deletes compute-heavy resources while preserving ADLS storage. To resume, re-run `provision_resources.sh` and remount in Databricks.

### Full Teardown

```bash
az group delete --name rg-ecommerce-lakehouse --yes
```

> This is irreversible. Only run when completely finished with the project.

## What's Next?

The E-Commerce Data Lakehouse is now complete. Review the [portfolio summary](../docs/portfolio_summary.md) for interview preparation and talking points.
