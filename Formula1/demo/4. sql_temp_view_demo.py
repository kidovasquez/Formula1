# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using SQL
# MAGIC
# MAGIC ### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_race_results where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("select * from v_race_results where race_year = 2019")