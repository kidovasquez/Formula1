# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
d_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("data_source","")
d_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("year",IntegerType(),True),
                                     StructField("round",IntegerType(),True),
                                     StructField("circuitId",IntegerType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("date",DateType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("url",StringType(),True)
])


# COMMAND ----------

races_df = spark.read.option("header",True) \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{d_file_date}/races.csv') 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Ingestion data and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat,lit,to_timestamp

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                                    .withColumn("data_source",lit(d_data_source)).withColumn("file_date",lit(d_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 -Select only required columns

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC