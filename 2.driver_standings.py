# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.parquet("/mnt/formulaonedll/presentation/race_results")\
.filter(f"file_date='{v_file_date}'")

# COMMAND ----------

race_yearlist = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet("/mnt/formulaonedll/presentation/race_results")\
.filter(col("race_year").isin(race_yearlist))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team").agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))



# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("Rank", rank().over(driver_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

