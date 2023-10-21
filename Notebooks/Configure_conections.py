# Databricks notebook source
# MAGIC %python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC        "fs.azure.account.oauth2.client.id": "c8975169-5922-4613-9127-b9166de31cd8",
# MAGIC        "fs.azure.account.oauth2.client.secret": "-mz8Q~7l66wZJIFbrF0WehUGTShCE8vC~l~VvbiP",
# MAGIC        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1a463d53-e4dc-4952-bf3c-6bac1d376e2d/oauth2/token",
# MAGIC        "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC     source = "abfss://datalake@dataalura.dfs.core.windows.net/",
# MAGIC     mount_point = "/mnt/datalake/data",
# MAGIC     extra_configs = configs)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/mnt/datalake/data"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/data/Imoveis"))
