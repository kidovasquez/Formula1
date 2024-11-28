# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file using spark datafram reader API

# COMMAND ----------

dbutils.widgets.text("data_source","")
d_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28")
d_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pitstops_schema)\
  .option("multiline", True)\
  .json(f"{raw_folder_path}/{d_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns and add new columns
# MAGIC 1. Rename raceId and driverId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_renamed = pit_stops_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id").withColumn("data_source",lit(d_data_source)).withColumn("file_date",lit(d_file_date))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

# write_data(pit_stops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.stop = src.stop and tgt.driver_id = src.driver_id"
merge_delta_data(pit_stops_final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops