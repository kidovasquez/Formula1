# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using spark datafram reader API

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

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("postion", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema)\
  .csv(f"{raw_folder_path}/{d_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns and add new columns
# MAGIC 1. Rename raceId and driverId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final = lap_times_df.withColumnRenamed("raceId","race_id")\
                                .withColumnRenamed("driverId","driver_id").withColumn("data_source",lit(d_data_source)).withColumn("file_date",lit(d_file_date))

# COMMAND ----------

lap_time_df = add_ingestion_date(lap_times_final)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

# write_data(lap_time_df,"f1_processed" ,"lap_times","race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.lap = src.lap and tgt.driver_id = src.driver_id"
merge_delta_data(lap_time_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")