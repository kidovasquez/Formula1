-- Databricks notebook source
create database if not exists f1_processed
location "dbfs:/mnt/formula1dlkaran/processed"

-- COMMAND ----------

desc database f1_processed;