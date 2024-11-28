-- Databricks notebook source
-- Create the f1_presentation database if it does not exist
-- Specify the location for the database
create database if not exists f1_presentation
location "dbfs:/mnt/formula1dlkaran/presentation"

-- COMMAND ----------

-- Describe the f1_presentation database
desc database f1_presentation;