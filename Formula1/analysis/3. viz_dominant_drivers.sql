-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Define the HTML string
-- MAGIC html = """<h1 style="color:Black;text-align:center;font:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC
-- MAGIC # Display the HTML string
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- Create or replace a temporary view named v_dominant_drivers
create or replace temp view v_dominant_drivers
as
-- Select driver name, total races, total points, average points, and rank based on average points
select driver_name,
  count(1) as total_races, -- Count the total number of races for each driver
  sum(calculated_points) as total_points, -- Sum the calculated points for each driver
  avg(calculated_points) as avg_points, -- Calculate the average points for each driver
  rank() over (order by avg(calculated_points) desc) driver_rank -- Rank drivers based on average points in descending order
from f1_presentation.calculated_race_results
group by driver_name -- Group by driver name
having total_races > 50 -- Only include drivers with more than 50 races
order by avg_points desc -- Order the results by average points in descending order

-- COMMAND ----------

-- Select race year, driver name, total races, total points, and average points for top 10 dominant drivers
select race_year,
  driver_name,
  count(1) as total_races, -- Count the total number of races for each driver in each year
  sum(calculated_points) as total_points, -- Sum the calculated points for each driver in each year
  avg(calculated_points) as avg_points -- Calculate the average points for each driver in each year
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) -- Filter for top 10 dominant drivers
group by race_year, driver_name -- Group by race year and driver name
order by race_year, avg_points desc -- Order the results by race year and average points in descending order

-- COMMAND ----------

-- Select race year, driver name, total races, total points, and average points for top 10 dominant drivers
select race_year,
  driver_name,
  count(1) as total_races, -- Count the total number of races for each driver in each year
  sum(calculated_points) as total_points, -- Sum the calculated points for each driver in each year
  avg(calculated_points) as avg_points -- Calculate the average points for each driver in each year
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) -- Filter for top 10 dominant drivers
group by race_year, driver_name -- Group by race year and driver name
order by race_year, avg_points desc -- Order the results by race year and average points in descending order