# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (internal table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database if not exists f1_demo
# MAGIC location "dbfs:/mnt/formula1dlkaran/demo"

# COMMAND ----------

result_df = spark.read.option("inferSchema", "true").json("dbfs:/mnt/formula1dlkaran/raw/2021-03-28/results.json")

# COMMAND ----------

result_df.write.format('delta').mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

result_df.write.format('delta').mode("overwrite").save('dbfs:/mnt/formula1dlkaran/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location 'dbfs:/mnt/formula1dlkaran/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load('dbfs:/mnt/formula1dlkaran/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

result_df.write.format('delta').partitionBy('constructorId').mode('overwrite').saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where position <=10 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/formula1dlkaran/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  "position <=10" ,
  { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/formula1dlkaran/demo/results_managed')

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

driver_day1_df = spark.read \
  .option("inferSchema", "true")\
    .json('dbfs:/mnt/formula1dlkaran/raw/2021-03-28/drivers.json')\
    .filter("driverId <= 10")\
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("drivers_day_1")

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df = spark.read \
  .option("inferSchema", "true")\
    .json('dbfs:/mnt/formula1dlkaran/raw/2021-03-28/drivers.json')\
    .filter("driverId between 6 and 15")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("drivers_day_2")

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day3_df = spark.read \
  .option("inferSchema", "true")\
    .json('dbfs:/mnt/formula1dlkaran/raw/2021-03-28/drivers.json')\
    .filter("driverId between 1 and 5 or driverId between 16 and 20")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as t
# MAGIC USING drivers_day_1 as upd
# MAGIC ON t.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.dob = upd.dob,
# MAGIC     t.forename = upd.forename,
# MAGIC     t.surname = upd.surname,
# MAGIC     t.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as t
# MAGIC USING drivers_day_2 as upd
# MAGIC ON t.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.dob = upd.dob,
# MAGIC     t.forename = upd.forename,
# MAGIC     t.surname = upd.surname,
# MAGIC     t.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/formula1dlkaran/demo/drivers_merge')


deltaTable.alias('tgt') \
  .merge(
    driver_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob" : "upd.dob",
      "forename" : "upd.forename",
      "surname" : "upd.surname",
      "updatedDate" : "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId" : "upd.driverId",
      "dob" : "upd.dob",
      "forename" : "upd.forename",
      "surname" : "upd.surname",
      "createdDate" : "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC hISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-11-19T09:27:01.000+00:00'

# COMMAND ----------

df =spark.read.format('delta').option('timestampAsOf', '2024-11-19T09:27:01.000+00:00').load('dbfs:/mnt/formula1dlkaran/demo/drivers_merge'  )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-11-19T09:27:01.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-11-19T09:27:01.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using  f1_demo.drivers_merge version as of 3 src
# MAGIC on tgt.driverId = src.driverId
# MAGIC when not matched then
# MAGIC insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction log

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_txn (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn
# MAGIC where driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_txn

# COMMAND ----------

for driver in range(3,20):
  spark.sql(f"""insert into f1_demo.drivers_txn
            select * from f1_demo.drivers_merge where driverId={driver}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("dbfs:/mnt/formula1dlkaran/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`dbfs:/mnt/formula1dlkaran/demo/drivers_convert_to_delta_new`