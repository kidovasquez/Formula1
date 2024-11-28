# Databricks notebook source
# Run the notebook "1. ingest_circuits_file" with specified parameters and capture the result
v_result = dbutils.notebook.run("1. ingest_circuits_file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

dbutils.notebook.run("2. ingest_races_file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

dbutils.notebook.run("3. ingest_constructors_file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. ingest_drivers_file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. ingest_results_file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. Ingest_pit_stops file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. Ingest_lap_times file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. Ingest qualifying file",0,{"data_source" : "Ergast API", "file_date" : "2021-04-18"})

# COMMAND ----------

v_result