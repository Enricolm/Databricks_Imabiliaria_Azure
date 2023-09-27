// Databricks notebook source
dbutils.fs.ls("./")

// COMMAND ----------

dbutils.fs.mkdirs("/mnt/dados")

// COMMAND ----------

// MAGIC %python
// MAGIC display(dbutils.fs.ls("/mnt"))

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "*",
  "fs.azure.account.oauth2.client.secret" -> "*",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/eca496fb-0e8f-4630-80ab-7319a08eca6d/oauth2/token")
dbutils.fs.mount(
  source = "link",
  mountPoint = "/mnt/dados",
  extraConfigs = configs)

