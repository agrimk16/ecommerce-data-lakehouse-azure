# Creation Order — E-Commerce Data Lakehouse

Follow this order to build the project from scratch.

## Phase 1: Foundation

| Step | Guide                                              | What You'll Build                                                          |
| ---- | -------------------------------------------------- | -------------------------------------------------------------------------- |
| 1    | [01_environment_setup.md](01_environment_setup.md) | Resource Group, ADLS Gen2, Key Vault, Databricks, ADF, Synapse, Event Hubs |

## Phase 2: Data Ingestion

| Step | Guide                                        | What You'll Build                                         |
| ---- | -------------------------------------------- | --------------------------------------------------------- |
| 2    | [02_data_ingestion.md](02_data_ingestion.md) | Kaggle dataset download, upload to ADLS Gen2 Bronze layer |

## Phase 3: Databricks Configuration

| Step | Guide                                            | What You'll Build                                   |
| ---- | ------------------------------------------------ | --------------------------------------------------- |
| 3    | [03_databricks_setup.md](03_databricks_setup.md) | Cluster, Secret Scope, ADLS mount, notebook imports |

## Phase 4: Medallion Architecture Transforms

| Step | Guide                                            | What You'll Build                                             |
| ---- | ------------------------------------------------ | ------------------------------------------------------------- |
| 4    | [04_bronze_to_silver.md](04_bronze_to_silver.md) | 5 PySpark notebooks — cleansing, dedup, type casting to Delta |
| 5    | [05_silver_to_gold.md](05_silver_to_gold.md)     | Star Schema modeling — Fact + 4 Dimensions, SCD Type 2        |

## Phase 5: Orchestration & Analytics

| Step | Guide                                                        | What You'll Build                                           |
| ---- | ------------------------------------------------------------ | ----------------------------------------------------------- |
| 6    | [06_pipeline_orchestration.md](06_pipeline_orchestration.md) | ADF linked services, datasets, pipelines, triggers          |
| 7    | [07_synapse_analytics.md](07_synapse_analytics.md)           | Synapse external tables, views, stored procedures, Power BI |

## Phase 6: Production Readiness

| Step | Guide                                        | What You'll Build                                      |
| ---- | -------------------------------------------- | ------------------------------------------------------ |
| 8    | [08_streaming_cicd.md](08_streaming_cicd.md) | Event Hubs streaming, Azure DevOps CI/CD, pytest suite |

## Cost Management Tips

- **Databricks auto-terminate** after 30 minutes of inactivity (~60–70% savings with spot instances)
- **Standard Databricks tier** instead of Premium saves ~40%
- **Synapse Serverless SQL** — no idle cost, pay only per TB scanned
- **Delete Event Hubs** after testing streaming (~$22/month saving)
- **LRS storage redundancy** — 40% cheaper than GRS, sufficient for dev
- **Run teardown script** when taking breaks longer than a day
- **Set a $30 budget alert** — safety net against surprise charges
