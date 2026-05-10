# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen2 Storage to Databricks
# MAGIC
# MAGIC This notebook mounts your Azure Data Lake Storage Gen2 containers (bronze, silver, gold)
# MAGIC to Databricks using a Service Principal or Account Key stored in Azure Key Vault.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Azure Key Vault with storage account key or Service Principal credentials
# MAGIC - Databricks secret scope linked to Key Vault
# MAGIC
# MAGIC **Run this once** to set up mounts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Variables

# COMMAND ----------

# Replace with your actual values
storage_account_name = "<your-storage-account-name>"
key_vault_scope = "ecommerce-kv-scope"       # Databricks secret scope name
storage_key_secret = "storage-account-key"     # Secret name in Key Vault

# Container names matching our Medallion architecture
containers = ["bronze", "silver", "gold", "config"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Mount Points

# COMMAND ----------

def mount_adls_container(container_name, storage_account, scope, secret_name):
    """
    Mount an ADLS Gen2 container to /mnt/{container_name}
    Uses account key from Databricks secret scope (backed by Key Vault)
    """
    mount_point = f"/mnt/{container_name}"

    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"  ✓ {mount_point} already mounted — skipping")
        return

    # Mount using account key
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net":
                dbutils.secrets.get(scope=scope, key=secret_name)
        }
    )
    print(f"  ✓ Mounted {container_name} → {mount_point}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Mount All Containers

# COMMAND ----------

print("Mounting ADLS Gen2 containers...")
print("=" * 50)

for container in containers:
    mount_adls_container(
        container_name=container,
        storage_account=storage_account_name,
        scope=key_vault_scope,
        secret_name=storage_key_secret
    )

print("=" * 50)
print("All mounts complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Mounts

# COMMAND ----------

# List all mount points
display(dbutils.fs.mounts())

# COMMAND ----------

# Test Bronze layer
dbutils.fs.ls("/mnt/bronze/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Using Service Principal (Recommended for Production)
# MAGIC
# MAGIC ```python
# MAGIC # Service Principal approach (uncomment to use)
# MAGIC configs = {
# MAGIC     "fs.azure.account.auth.type": "OAuth",
# MAGIC     "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC     "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope=key_vault_scope, key="sp-client-id"),
# MAGIC     "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=key_vault_scope, key="sp-client-secret"),
# MAGIC     "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope=key_vault_scope, key='tenant-id')}/oauth2/token"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unmount (if needed)
# MAGIC
# MAGIC ```python
# MAGIC # Only run if you need to remount
# MAGIC for container in containers:
# MAGIC     dbutils.fs.unmount(f"/mnt/{container}")
# MAGIC ```
