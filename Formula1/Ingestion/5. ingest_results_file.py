# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest results.json File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file

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

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType
)

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{d_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and ingest date
# MAGIC 1. resultId to result_id
# MAGIC 2. raceId to race_id
# MAGIC 3. driverId to driver_id
# MAGIC 4. contructorID to constructor_id
# MAGIC 5. positionText to position_text
# MAGIC 6. postionOrder to position_order
# MAGIC 7. fastestLap to fastest_lap
# MAGIC 8. fastestLapTime to fastest_lap_time
# MAGIC 9. fastestLapSpeed to fastest_lap_speed
# MAGIC 10. Add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

renamed_results_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source",lit(d_data_source)) \
    .withColumn("file_date",lit(d_file_date))

# COMMAND ----------

final_df = add_ingestion_date(renamed_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop statusId column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = final_df.drop(col('statusId'))

# COMMAND ----------

result_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the dataframe in parquet format partioned by race year

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# write_data(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(result_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")