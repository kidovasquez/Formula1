-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

create database if not exists f1_raw

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
using csv
options (path "dbfs:/mnt/formula1dlkaran/raw/circuits.csv", header "true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceID int,
  year int,
  round int,
  circuitId INT,
  name STRING,
  date date,
  time string,
  url STRING
)
using csv
options (path "dbfs:/mnt/formula1dlkaran/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables from JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name string,
  nationality string,
  url string)
  using json
  options (path "dbfs:/mnt/formula1dlkaran/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number int,
  code string,
  name Struct<forename:string, surname:string>,
  dob date,
  nationality string,
  url string
)
using json
options (path "dbfs:/mnt/formula1dlkaran/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Results table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points double,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed string,
  statusId int
)
using json
options (path "dbfs:/mnt/formula1dlkaran/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PitStops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  raceId int,
  driverId int,
  stop int,
  lap int,
  time string,
  duration string,
  milliseconds int
)
using json
options (path "dbfs:/mnt/formula1dlkaran/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Craete Lap Times Table
-- MAGIC - CSV file
-- MAGIC - Multiple fles

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options (path "dbfs:/mnt/formula1dlkaran/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Qualifying Table
-- MAGIC - JSON file
-- MAGIC - Multiline JSON
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string
)
using json
options (path "dbfs:/mnt/formula1dlkaran/raw/qualifying",multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------

show databases

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

show tables;