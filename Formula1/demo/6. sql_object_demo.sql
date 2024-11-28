-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. Show command
-- MAGIC 5. Describe command
-- MAGIC 6. Find the current database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC 1. Create managed table using python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python_1")

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020 ;

-- COMMAND ----------

create table race_results_sql
as 
select * from demo.race_results_python
where race_year = 2020 ;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent Temp View

-- COMMAND ----------

create or replace temp view v_race_results
as
select * from demo.race_results_python
where race_year = 2018

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select * from demo.race_results_python
where race_year = 2018

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace view demo.pv_race_results
as
select * from demo.race_results_python
where race_year = 2022

-- COMMAND ----------

show tables in demo