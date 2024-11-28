-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Create bronze tables
-- MAGIC 1. drivers.json
-- MAGIC 2. results.json
-- MAGIC bronze folder path 'abfss://bronze@databricksucexternal.dfs.core.windows.net

-- COMMAND ----------

drop table if exists formula1_dev.bronze.drivers;

create table if not exists formula1_dev.bronze.drivers(
  driverId int,
  driverRef String,
  number int,
  code string,
  name struct<forename: string,surname: string>,
  dob date,
  nationality string,
  url string
)
using json
options (path 'abfss://bronze@databricksucexternal.dfs.core.windows.net/drivers.json')

-- COMMAND ----------

drop table if exists formula1_dev.bronze.results;

create table if not exists formula1_dev.bronze.results(
  resultId INT,
  raceId INT,
  driverId INT,
  contructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points int,
  laps INT,
  time string,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestlapTime string,
  fastestLapSpeed string,
  statusId INT
)
using json
options (path 'abfss://bronze@databricksucexternal.dfs.core.windows.net/results.json')