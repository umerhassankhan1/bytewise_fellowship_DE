-- Databricks notebook source
CREATE DATABASE demo;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;


-- COMMAND ----------

USE demo; 

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Manage Tables in SPARK

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"dbfs:/{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DESCRIBE race_results_python;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE race_results_sql;

-- COMMAND ----------

SHOW TABLEs IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### EXTERNAL Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"dbfs:/{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formulaonedll/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

INSERT INTO  demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * FROM demo.race_results_python
WHERE race_year =2019;


-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * FROM demo.race_results_python
WHERE race_year =2019;


-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;


-- COMMAND ----------

CREATE OR REPLACE VIEW demo.p_race_results
AS 
SELECT * FROM demo.race_results_python
WHERE race_year =2019;


-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

 