# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join circuits data and races data using joins

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
  .filter("circuit_id < 70")\
  .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019").withColumnRenamed("name","race_name")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
  .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Joins

# COMMAND ----------

# Left Outer join
race_circuits_left_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left")\
  .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Right Outer join
race_circuits_right_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right")\
  .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# Full Outer join
race_circuits_full_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"full")\
  .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Joins

# COMMAND ----------

# Semi join
race_circuits_semi_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Joins

# COMMAND ----------

# Anti join
race_circuits_anti_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Joins

# COMMAND ----------

# Cross Join
race_circuits_cross_df = circuits_df.crossJoin(races_df)