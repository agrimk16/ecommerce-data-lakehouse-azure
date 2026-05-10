# E-Commerce Data Lakehouse — Portfolio Summary

## Project Title

**Real-Time E-Commerce Data Lakehouse on Azure**

## One-Line Summary

End-to-end Medallion Architecture processing 100K+ e-commerce orders through Bronze → Silver → Gold layers using Databricks, Delta Lake, Star Schema modeling, ADF orchestration, Synapse Serverless SQL, real-time streaming, and CI/CD automation.

## Business Problem

An e-commerce company needs to:

- Consolidate fragmented order, customer, product, and seller data into a unified analytics platform
- Track revenue trends, delivery performance, and customer segments for executive dashboards
- Process real-time clickstream events for live user behavior analytics
- Ensure data quality and auditability across all transformation layers
- Automate daily refreshes with zero manual intervention

## Technical Highlights

| Component          | Technology                              | Pattern                                                    |
| ------------------ | --------------------------------------- | ---------------------------------------------------------- |
| **Ingestion**      | Azure Data Factory, Kaggle API          | Parameterized copy pipelines, config-driven sources        |
| **Storage**        | Azure Data Lake Storage Gen2            | Hierarchical namespace, Bronze/Silver/Gold containers      |
| **Processing**     | Azure Databricks (PySpark)              | Medallion Architecture, Delta Lake ACID transactions       |
| **Data Modeling**  | Star Schema (Fact + 4 Dimensions)       | SCD Type 2 via Delta MERGE, surrogate keys                 |
| **Orchestration**  | Azure Data Factory                      | Master pipeline, dependency chains, schedule triggers      |
| **Serving**        | Azure Synapse Serverless SQL            | External tables on Delta, views, stored procedures         |
| **Streaming**      | Event Hubs + Spark Structured Streaming | Checkpointed micro-batch, Kafka-compatible protocol        |
| **Visualization**  | Power BI (DirectQuery)                  | Revenue trends, category breakdown, delivery KPIs, geo map |
| **Infrastructure** | Bicep Templates                         | Modular IaC — 6 resource modules, parameterized            |
| **CI/CD**          | Azure DevOps (YAML Pipelines)           | Multi-stage: test → deploy, Key Vault variable groups      |
| **Testing**        | pytest + PySpark                        | Layer-specific test suites (Bronze, Silver, Gold)          |
| **Security**       | Azure Key Vault, Managed Identity, RBAC | Zero hardcoded secrets, least-privilege access             |

## Architecture Pattern

**Medallion Architecture (Lambda Extension)** — Batch processing through Bronze → Silver → Gold with a parallel real-time streaming path via Event Hubs + Spark Structured Streaming, served through Synapse Serverless SQL.

## Key Interview Discussion Points

1. **Why Medallion Architecture?** Clear separation of concerns — raw ingestion (Bronze), data quality (Silver), business logic (Gold). Each layer is independently testable and reprocessable.

2. **Delta Lake over plain Parquet?** ACID transactions enable safe concurrent writes, MERGE enables SCD Type 2 upserts, time travel enables auditing and rollback, schema enforcement prevents bad data from entering production tables.

3. **SCD Type 2 design choice:** Customer addresses change over time. Using `valid_from`, `valid_to`, and `is_current` columns lets us report historical orders against the address at the time of purchase — not the customer's current address.

4. **Why Star Schema in the Gold layer?** Optimized for analytical queries — single fact table with clean foreign key joins to dimensions. BI tools generate efficient SQL. Denormalized dimensions reduce join complexity.

5. **Synapse Serverless vs. Dedicated?** Serverless has zero idle cost — charges only per TB scanned. For a portfolio project with periodic queries, this saves hundreds per month compared to a dedicated pool.

6. **Streaming architecture:** Event Hubs provides Kafka-compatible ingestion. Spark Structured Streaming with checkpointing ensures exactly-once processing. Separating batch and streaming paths (Lambda) means batch quality is never compromised by streaming latency.

7. **CI/CD strategy:** YAML pipeline runs pytest on every PR (gate), then deploys Bicep infrastructure on merge to main. Variable groups linked to Key Vault ensure secrets never appear in code or pipeline logs.

8. **Data quality testing:** Three-tier test suite — Bronze tests validate raw schema contracts, Silver tests verify cleansing logic (dedup, type casting), Gold tests ensure referential integrity and SCD Type 2 correctness.

## Skills Demonstrated

- [x] Medallion Architecture (Bronze → Silver → Gold)
- [x] PySpark data transformations at scale
- [x] Delta Lake (MERGE, time travel, schema enforcement)
- [x] Star Schema dimensional modeling
- [x] SCD Type 2 slowly changing dimensions
- [x] Azure Data Factory pipeline orchestration
- [x] Azure Synapse Serverless SQL analytics
- [x] Real-time streaming (Event Hubs + Structured Streaming)
- [x] Power BI DirectQuery dashboards
- [x] Infrastructure as Code (Bicep)
- [x] CI/CD automation (Azure DevOps YAML pipelines)
- [x] Automated testing (pytest)
- [x] Security best practices (Key Vault, Managed Identity, RBAC)
- [x] Cost optimization (spot instances, auto-terminate, serverless, teardown scripts)

## Azure Services Used

| Service                 | Role                                                 |
| ----------------------- | ---------------------------------------------------- |
| Azure Data Lake Gen2    | Multi-layer data lake (Bronze, Silver, Gold)         |
| Azure Databricks        | PySpark processing, Delta Lake, streaming            |
| Azure Data Factory      | Pipeline orchestration, scheduling, monitoring       |
| Azure Synapse Analytics | Serverless SQL queries on Gold Delta tables          |
| Azure Event Hubs        | Real-time clickstream event ingestion                |
| Azure Key Vault         | Secret management (storage keys, connection strings) |
| Azure DevOps            | CI/CD pipelines, automated testing                   |
| Power BI                | Executive dashboards via DirectQuery                 |

## Dataset

**Brazilian E-Commerce (Olist)** from Kaggle — ~100K orders, 113K order items, 99K customers, 33K products, plus simulated clickstream events.
