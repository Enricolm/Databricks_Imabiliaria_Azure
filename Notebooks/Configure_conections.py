# Databricks notebook source
# MAGIC %python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC        "fs.azure.account.oauth2.client.id": "3e2d2549-4408-4aab-90a0-ef6f1d52c04f",
# MAGIC        "fs.azure.account.oauth2.client.secret": "tFX8Q~LVnBOGoTVy6tnDVi~wpjb0v~NdNvDGIaF3",
# MAGIC        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1a463d53-e4dc-4952-bf3c-6bac1d376e2d/oauth2/token",
# MAGIC        "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC     source = "abfss://datalake@basealura.dfs.core.windows.net/",
# MAGIC     mount_point = "/mnt/datalake/data",
# MAGIC     extra_configs = configs)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/mnt/data/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/data/Raw"))

# COMMAND ----------

dados_raw = spark.read.json("/mnt/data/Raw/")

# COMMAND ----------

display(dados_raw)

# COMMAND ----------

import pyspark.sql.functions as f
dados_without_col = dados_raw.drop("imagens", "usuario")
display(dados_without_col)

# COMMAND ----------

dados_without_col.printSchema()

# COMMAND ----------

dados_format = dados_without_col.withColumn("id", f.col("anuncio.id"))
display(dados_format)

# COMMAND ----------

dados_format.write.format("parquet")\
    .mode("overwrite")\
    .save("/mnt/data/Bronze/")

# COMMAND ----------


