# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest contructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the json file using spark reader dataframe

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

constructors_schema = "constructorId int,constructorRef string, name string, nationality string, url string"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{d_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwantanted columns from dataframe

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

constructors_dropdf = constructors_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename Columns and add ingestion date

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_dropdf).withColumnRenamed('constructorId',"constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("data_source",lit(d_data_source)).withColumn("file_date",lit(d_file_date))
                                            

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")