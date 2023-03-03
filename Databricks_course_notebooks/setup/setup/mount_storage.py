# Databricks notebook source
storage_account_name = "formulaonedll"
client_id = dbutils.secrets.get(scope="formulaone_scope", key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="formulaone_scope", key="databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formulaone_scope", key="databricks-app-client-secret")

# COMMAND ----------

config = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = config)   

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulaonedll/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulaonedll/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formulaonedll/presentation")

# COMMAND ----------

