# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df_filtered = races_df.filter("race_year = 2019 and round <=5")

# COMMAND ----------

display(races_df_filtered)