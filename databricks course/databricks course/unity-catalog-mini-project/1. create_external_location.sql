-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Create the external locations required for the project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

create external location databricksucexternal_bronze
url 'abfss://bronze@databricksucexternal.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage-credentials`);

-- COMMAND ----------

desc external location databricksucexternal_bronze;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://bronze@databricksucexternal.dfs.core.windows.net/'

-- COMMAND ----------

create external location databricksucexternal_silver
url 'abfss://silver@databricksucexternal.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage-credentials`);

-- COMMAND ----------

create external location databricksucexternal_gold
url 'abfss://gold@databricksucexternal.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage-credentials`);