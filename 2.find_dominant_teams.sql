-- Databricks notebook source
SELECT team_name, COUNT(*) AS total_races,SUM(calculated_points) AS total_points, AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
ORDER BY avg_points DESC

-- COMMAND ----------

