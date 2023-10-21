# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

dados_raw = spark.read.json("/mnt/datalake/data/Imoveis/Raw/")
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


