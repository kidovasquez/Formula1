# Databricks notebook source
dbutils.widgets.text("file_date",'2021-03-21')
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

spark.sql(f"""
          create table if not exists f1_presentation.calculated_race_results
          (
            race_year int,
            team_name string,
            driver_id int,
            driver_name string,
            race_id int,
            position int,
            points int,
            calculated_points int,
            created_date timestamp,
            updated_date timestamp
          )
          using delta
""")

# COMMAND ----------

#%sql
#create table f1_presentation.calculated_race_results
#using parquet 
#as
#select races.race_year,
#        constructors.name as team_name,
#        drivers.name as driver_name,
#        results.position,
#        results.points,
#        11-results.position as calculated_points
#  from f1_processed.results
#  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
#  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
#  join f1_processed.races on (results.race_id = races.race_id)
#where results.position <= 10

# COMMAND ----------

spark.sql(f"""
create or replace temp view race_results_updated
as
select races.race_year,
        constructors.name as team_name,
        drivers.driver_id,
        drivers.name as driver_name,
        races.race_id,
        results.position,
        results.points,
        11-results.position as calculated_points
  from f1_processed.results
  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
  join f1_processed.races on (results.race_id = races.race_id)
where results.position <= 10
and results.file_date = '{file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_results_updated

# COMMAND ----------

spark.sql(f"""
MERGE INTO f1_presentation.calculated_race_results as t
USING race_results_updated as upd
ON (t.driver_id = upd.driver_id and t.race_id = upd.race_id)
WHEN MATCHED THEN
  UPDATE SET
    t.position = upd.position,
    t.points = upd.points,
    t.calculated_points = upd.calculated_points,
    t.updated_date = current_timestamp
WHEN NOT MATCHED
  THEN INSERT (
    race_year,team_name, driver_id,driver_name,race_id,position,points,calculated_points,created_date 
  )
  VALUES (
    race_year,team_name, driver_id,driver_name,race_id,position,points,calculated_points,current_timestamp
  )
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation.calculated_race_results