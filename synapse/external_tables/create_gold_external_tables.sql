-- ============================================================
-- Synapse Serverless SQL: External Tables on Gold Delta Layer
-- ============================================================
-- These external tables allow querying Gold Delta Lake tables
-- directly from Synapse SQL (Serverless) without data movement.
-- ============================================================

-- Step 1: Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ecommerce_gold')
    CREATE DATABASE ecommerce_gold;
GO

USE ecommerce_gold;
GO

-- Step 2: Create Master Key (required for external data source)
-- Replace <your-master-key-password> with a strong password (store it in your Key Vault)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your-master-key-password>';
GO

-- Step 3: Create Database Scoped Credential (using Managed Identity)
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'SynapseIdentity')
    CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
    WITH IDENTITY = 'Managed Identity';
GO

-- Step 4: Create External Data Source (pointing to Gold container)
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldDataLake')
    CREATE EXTERNAL DATA SOURCE GoldDataLake
    WITH (
        -- Replace <storage-account-name> with your ADLS Gen2 account name
        -- e.g. 'abfss://gold@ecomlhadlsabc123.dfs.core.windows.net'
        LOCATION = 'abfss://gold@<storage-account-name>.dfs.core.windows.net',
        CREDENTIAL = SynapseIdentity
    );
GO

-- Silver data source (needed for order_items join in vw_sales_by_category)
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'SilverDataLake')
    CREATE EXTERNAL DATA SOURCE SilverDataLake
    WITH (
        LOCATION = 'abfss://silver@<storage-account-name>.dfs.core.windows.net',
        CREDENTIAL = SynapseIdentity
    );
GO

-- Step 5: Create External File Format (Delta)
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'DeltaFormat')
    CREATE EXTERNAL FILE FORMAT DeltaFormat
    WITH (
        FORMAT_TYPE = DELTA
    );
GO

-- ============================================================
-- External Tables
-- ============================================================

-- Fact Orders
CREATE OR ALTER VIEW fact_orders AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_orders/',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'DELTA'
) AS orders;
GO

-- Dim Customers
CREATE OR ALTER VIEW dim_customers AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_customers/',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'DELTA'
) AS customers;
GO

-- Dim Products
CREATE OR ALTER VIEW dim_products AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_products/',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'DELTA'
) AS products;
GO

-- Dim Date
CREATE OR ALTER VIEW dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_date/',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'DELTA'
) AS dates;
GO

-- Dim Geography
CREATE OR ALTER VIEW dim_geography AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_geography/',
    DATA_SOURCE = 'GoldDataLake',
    FORMAT = 'DELTA'
) AS geo;
GO

-- Add external view for Silver order_items (needed by vw_sales_by_category)
CREATE OR ALTER VIEW order_items AS
SELECT *
FROM OPENROWSET(
    BULK 'order_items/',
    DATA_SOURCE = 'SilverDataLake',
    FORMAT = 'DELTA'
) AS items;
GO

PRINT 'All external views created successfully!';
