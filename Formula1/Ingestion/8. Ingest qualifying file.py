# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

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

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualify_df = spark.read.schema(qualifying_schema)\
  .option("multiline",True)\
  .json(f"{raw_folder_path}/{d_file_date}/qualifying/qualifying_split*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns and add new columns
# MAGIC 1. Rename qualifyingId, raceId, driverId, constructorId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualify_df_add = add_ingestion_date(qualify_df)

# COMMAND ----------

qualify_final_df = qualify_df_add.withColumnRenamed("qualifyId","qualify_id")\
                             .withColumnRenamed("raceId","race_id")\
                             .withColumnRenamed("driverId","driver_id")\
                             .withColumnRenamed("constructorId","constructor_id")\
                               .withColumn("data_source",lit(d_data_source)) \
                               .withColumn("file_date",lit(d_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

# write_data(qualify_final_df, "f1_processed", "qualify", "race_id")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id and tgt.race_id = src.race_id"
merge_delta_data(qualify_final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")