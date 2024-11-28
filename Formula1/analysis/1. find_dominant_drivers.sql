-- Databricks notebook source
-- Select driver name, total races, total points, and average points
select driver_name,
  count(1) as total_races, -- Count the number of races for each driver
  sum(calculated_points) as total_points, -- Sum the points for each driver
  avg(calculated_points) as avg_points -- Calculate the average points for each driver
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020 -- Filter results for races between 2011 and 2020
group by driver_name
having total_races > 50 -- Only include drivers with more than 50 races
order by total_points desc -- Order the results by total points in descending order

-- COMMAND ----------

select driver_name,
  count(1) as total_races, -- Count the number of races for each driver
  sum(calculated_points) as total_points, -- Sum the points for each driver
  avg(calculated_points) as avg_points -- Calculate the average points for each driver
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010 -- Filter results for races between 2001 and 2010
group by driver_name
having total_races > 50 -- Only include drivers with more than 50 races
order by total_points desc -- Order the results by total points in descending order