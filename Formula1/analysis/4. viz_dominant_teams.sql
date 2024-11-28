-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Define HTML content for the report header
-- MAGIC html = """<h1 style="color:Black;text-align:center;font:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC
-- MAGIC # Display the HTML content
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- Create or replace a temporary view named v_dominant_teams
create or replace temp view v_dominant_teams
as
-- Select team name, total races, total points, average points, and rank based on average points
select team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  rank() over (order by avg(calculated_points) desc) team_rank
from f1_presentation.calculated_race_results
-- Group by team name to aggregate the results
group by team_name
-- Filter to include only teams with at least 100 races
having total_races >= 100
-- Order the results by average points in descending order
order by avg_points desc

-- COMMAND ----------

-- Select race statistics for top 5 dominant teams by year
select race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
-- Filter to include only the top 5 dominant teams based on average points
where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
-- Group by race year and team name to aggregate the results
group by race_year, team_name
-- Order by race year and average points in descending order
order by race_year, avg_points desc

-- COMMAND ----------

-- Select race year, team name, total races, total points, and average points for top 5 dominant teams
select race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
-- Filter to include only the top 5 dominant teams based on average points
where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
group by race_year, team_name
-- Order by race year and average points in descending order
order by race_year, avg_points desc