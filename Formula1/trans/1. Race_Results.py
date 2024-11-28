# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name","race_name")\
  .withColumnRenamed("race_timestamp","race_date")\
  .select("race_id","race_year","race_name","race_date","circuit_id")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
  .withColumnRenamed("location","circuit_location")\
  .select("circuit_id","circuit_location")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number")\
  .withColumnRenamed("nationality","driver_nationality")\
  .select("driver_id","driver_name","driver_number","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")\
  .select("constructor_id","team")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
  .filter(f"file_date = '{file_date}'")\
  .withColumnRenamed("time","race_time")\
  .withColumnRenamed("race_id","result_race_id")\
  .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

race_circuit_join = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id)

# COMMAND ----------

results_join_table = results_df.join(drivers_df,drivers_df.driver_id == results_df.driver_id).join(constructors_df,constructors_df.constructor_id == results_df.constructor_id).join(race_circuit_join,race_circuit_join.race_id == results_df.result_race_id)

# COMMAND ----------

final_df = results_join_table.select("race_id","race_year","race_name","race_date","circuit_location","driver_name",
                                     "driver_number","driver_nationality","team","grid","fastest_lap",
                                     "race_time","points","position","result_file_date").withColumn("created_date",current_timestamp()).withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_presentation','race_results',presentation_folder_path,merge_condition,'race_id')