-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Drop all the tables

-- COMMAND ----------

DROP DATABASE if EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/mnt/formula1dlkaran/processed"

-- COMMAND ----------

DROP DATABASE if EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "dbfs:/mnt/formula1dlkaran/presentation"