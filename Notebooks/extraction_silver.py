# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# COMMAND ----------

dados_Silver_not_transformated = spark.read.parquet|("/mnt/datalake/data/Imoveis/Bronze")

# COMMAND ----------

display(dados_Silver_not_transformated)

# COMMAND ----------

display(dados_Silver_not_transformated.printSchema())

# COMMAND ----------

#Removing columns where we have property information
display(dados_Silver_not_transformated.select("anuncio.*"))
dados_without_caracteristicas = dados_Silver_not_transformated.select("anuncio.*")

dados_without_caracteristicas = dados_without_caracteristicas.drop("caracteristicas", "endereco")


# COMMAND ----------

display(dados_without_caracteristicas)

# COMMAND ----------

dados_without_caracteristicas = dados_without_caracteristicas.withColumn("area_total", 
f.col("area_total")[0])
dados_without_caracteristicas = dados_without_caracteristicas.withColumn("area_util", f.col("area_util")[0])
dados_without_caracteristicas = dados_without_caracteristicas.withColumn("banheiros",f.col("banheiros")[0])
dados_without_caracteristicas = dados_without_caracteristicas.withColumn("quartos", f.col("quartos")[0])
dados_without_caracteristicas = dados_without_caracteristicas.withColumn("suites", f.col("suites")[0])
dados_without_caracteristicas = dados_without_caracteristicas.withColumn('vaga', f.col('vaga')[0])
display(dados_without_caracteristicas)

# COMMAND ----------

schema = StructType([
    StructField("condominio", StringType()),
    StructField("iptu", StringType()),
    StructField("tipo", StringType()),
    StructField("valor", StringType())]
)

dados_transforme = dados_without_caracteristicas.withColumn("valores", f.col("valores")[0])

dados_transforme = dados_transforme.select("*",
    f.col("valores.condominio").alias("condominio"),
    f.col("valores.iptu").alias("iptu"),
    f.col("valores.tipo").alias("tipo"),
    f.col("valores.valor").alias("valor")
)

dados_transforme = dados_transforme.drop("valores")

dados_transforme= dados_transforme.na.fill("0")

display(dados_transforme)


# COMMAND ----------

dados_transforme.write.format("parquet")\
    .mode("overwrite")\
    .save("/mnt/datalake/data/Imoveis/Silver")

# COMMAND ----------


