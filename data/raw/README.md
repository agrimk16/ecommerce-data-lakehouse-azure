# Raw Data — Kaggle Dataset

## Download Instructions

1. Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Click "Download" (requires free Kaggle account)
3. Extract the ZIP and place all CSV files in this folder

Or use the Kaggle CLI:

```bash
pip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d .
```

## Expected Files

| File                                  | Records | Used In                                                           |
| ------------------------------------- | ------- | ----------------------------------------------------------------- |
| olist_orders_dataset.csv              | ~100K   | pl_ingest_orders → Bronze → Silver → Gold fact_orders             |
| olist_order_items_dataset.csv         | ~113K   | pl_ingest_orders → Bronze → Silver → Gold fact_orders             |
| olist_order_payments_dataset.csv      | ~104K   | pl_ingest_orders → Bronze → Silver (payments aggregation)         |
| olist_customers_dataset.csv           | ~99K    | pl_ingest_customers → Bronze → Silver → Gold dim_customers (SCD2) |
| olist_products_dataset.csv            | ~33K    | pl_ingest_products → Bronze → Silver → Gold dim_products          |
| olist_sellers_dataset.csv             | ~3K     | pl_ingest_products → Bronze                                       |
| olist_order_reviews_dataset.csv       | ~100K   | pl_ingest_customers → Bronze                                      |
| olist_geolocation_dataset.csv         | ~1M     | pl_ingest_customers → Bronze → Silver → Gold dim_geography        |
| product_category_name_translation.csv | ~71     | Used in products_bronze_to_silver for PT→EN translation           |

## Upload to Azure

After downloading, upload to the Bronze container:

```bash
# Using variables from setup/variables.sh
source setup/variables.sh
az storage blob upload-batch \
    --destination bronze \
    --source data/raw/ \
    --account-name "$DATALAKE_ACCOUNT" \
    --pattern "*.csv"
```

## Pipeline Config

See [data/configs/dataset_file_list.json](../configs/dataset_file_list.json) for the parameterized file list used by ADF Lookup + ForEach activities (same pattern as the COVID-19 project's `ecdc_file_list.json`).
