-- Databricks notebook source
-- Select team name, total races, total points, and average points
select team_name,
  count(1) as total_races, -- Count the number of races
  sum(calculated_points) as total_points, -- Sum of calculated points
  avg(calculated_points) as avg_points -- Average of calculated points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020 -- Filter races between 2011 and 2020
group by team_name
having total_races >= 100 -- Only include teams with 100 or more races
order by avg_points desc -- Order by average points in descending order