-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create catalogs and schemas required for the project
-- MAGIC 1. Catalog - formula1_dev(without managed locations)
-- MAGIC 2. Schemas - bronze,silver,gold(with managed locations)

-- COMMAND ----------

create catalog if not exists formula1_dev

-- COMMAND ----------

use catalog formula1_dev

-- COMMAND ----------

create schema if not exists bronze
managed location "abfss://bronze@databricksucexternal.dfs.core.windows.net/"

-- COMMAND ----------

create schema if not exists silver
managed location "abfss://silver@databricksucexternal.dfs.core.windows.net/"

-- COMMAND ----------

create schema if not exists gold
managed location "abfss://gold@databricksucexternal.dfs.core.windows.net/"