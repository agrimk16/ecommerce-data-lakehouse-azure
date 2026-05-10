# Step 2: Data Ingestion

## Concepts

The Bronze layer stores raw data exactly as received from the source — no transformations, no schema enforcement. This preserves the original data for auditability and reprocessing.

**Key Interview Concepts:**

- **Medallion Architecture**: Bronze (raw) → Silver (cleansed) → Gold (business-ready)
- **Immutable Landing Zone**: Never modify raw data — always write transforms to the next layer
- **Schema-on-Read**: Raw CSVs have no enforced schema; schema is applied during Silver processing

## 2.1 Download the Kaggle Dataset

The project uses the **Brazilian E-Commerce (Olist)** dataset from Kaggle (~100K orders, 113K items, 99K customers, 33K products).

### Install the Kaggle CLI

```bash
pip install kaggle
```

### Configure Kaggle API Key

1. Go to [kaggle.com/settings](https://www.kaggle.com/settings) → scroll to **API** → **Create New Token**
2. This downloads `kaggle.json`
3. Place it in the correct location:

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

### Download the Dataset

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/ --unzip
```

You should now have these files in `data/raw/`:

```
olist_customers_dataset.csv
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
olist_geolocation_dataset.csv
product_category_name_translation.csv
```

## 2.2 Upload to ADLS Gen2 Bronze Container

```bash
source setup/variables.sh

az storage blob upload-batch \
    --account-name $STORAGE_ACCOUNT \
    --destination "bronze/ecommerce" \
    --source data/raw/ \
    --pattern "*.csv" \
    --auth-mode login \
    --overwrite
```

### Verify the Upload

```bash
az storage blob list \
    --account-name $STORAGE_ACCOUNT \
    --container-name bronze \
    --prefix "ecommerce/" \
    --output table \
    --auth-mode login
```

You should see all 9 CSV files listed.

## 2.3 Upload Simulated Clickstream Data (Optional)

If you plan to test the streaming pipeline later, also upload the simulated clickstream reference data:

```bash
az storage blob upload-batch \
    --account-name $STORAGE_ACCOUNT \
    --destination "bronze/clickstream" \
    --source data/simulated/ \
    --pattern "*.csv" \
    --auth-mode login \
    --overwrite
```

## 2.4 Upload Pipeline Config

```bash
az storage blob upload-batch \
    --account-name $STORAGE_ACCOUNT \
    --destination "config" \
    --source data/configs/ \
    --auth-mode login \
    --overwrite
```

## What's Next?

Proceed to [03_databricks_setup.md](03_databricks_setup.md) to configure your Databricks workspace, create a cluster, and mount the data lake.
