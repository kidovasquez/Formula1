# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Data Lake using sas token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv.file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlkaran.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlkaran.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlkaran.dfs.core.windows.net","sp=rw&st=2024-11-06T11:20:58Z&se=2024-11-06T19:20:58Z&spr=https&sv=2022-11-02&sr=c&sig=chO5syWDx42yXMIKXKzgwgxTx1aXQ0%2F75W6iElsB0nw%3D")

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaran.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dlkaran.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlkaran.dfs.core.windows.net/circuits.csv"))