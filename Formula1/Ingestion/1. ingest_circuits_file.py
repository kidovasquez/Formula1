# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the csv file

# COMMAND ----------

# Display help for dbutils.widgets
dbutils.widgets.help()

# COMMAND ----------


dbutils.widgets.text("file_date","2021-03-21")

d_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

dbutils.widgets.text("data_source","")

d_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option("header", True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/{d_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitid","circuit_id")\
  .withColumnRenamed("circuitRef","circuit_ref")\
  .withColumnRenamed("lat","latitude")\
  .withColumnRenamed("lng","longitude")\
  .withColumnRenamed("alt","altitude")\
  .withColumn("data_source",lit(d_data_source))\
  .withColumn("file_date",lit(d_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

# Write the final DataFrame to a Delta table in overwrite mode
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")