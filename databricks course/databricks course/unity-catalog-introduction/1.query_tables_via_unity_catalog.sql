-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Query data via unity catalog using 3 level namespace

-- COMMAND ----------

select * from demo_catalog.demo_schema.circuits

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

show catalogs

-- COMMAND ----------

use catalog demo_catalog

-- COMMAND ----------

select current_schema()

-- COMMAND ----------

show schemas

-- COMMAND ----------

use demo_schema

-- COMMAND ----------

select * from circuits

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql('''show tables'''))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('demo_catalog.demo_schema.circuits')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)