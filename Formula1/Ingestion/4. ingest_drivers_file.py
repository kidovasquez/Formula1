# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)

])

# COMMAND ----------

driver_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name",name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{d_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns and add new columns
# MAGIC 1. driverid to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat,lit

# COMMAND ----------

drivers_with_columns_df = add_ingestion_date(drivers_df).withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_ref")\
                                    .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))\
                                      .withColumn("data_source",lit(d_data_source)).withColumn("file_date",lit(d_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Drop the unwanted columns
# MAGIC 1. forename
# MAGIC 2. surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the ouput to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")