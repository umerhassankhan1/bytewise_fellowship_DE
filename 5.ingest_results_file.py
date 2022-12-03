# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results.json File

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 Read the json file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                  StructField("raceId", IntegerType(), True),
                                  StructField("driverId", IntegerType(), True),
                                  StructField("constructorId", IntegerType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("grid", IntegerType(), True),
                                  StructField("position", IntegerType(), True),
                                  StructField("positionText", StringType(), True),
                                  StructField("positionOrder", IntegerType(), True),
                                  StructField("points", FloatType(), True),
                                  StructField("laps", IntegerType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("milliseconds", IntegerType(), True),
                                  StructField("fastestLap", IntegerType(), True),
                                  StructField("rank", IntegerType(), True),
                                  StructField("fastestLapTime", StringType(), True),
                                  StructField("fastestLapSpeed", FloatType(), True),
                                  StructField("statusId", StringType(), True),
    ])

# COMMAND ----------

results_read_df = spark.read.schema(results_schema).json(f'/mnt/formulaonedll/raw/{v_file_date}/results.json')

# COMMAND ----------

display(results_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_read_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order")    .withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Drop the unwanted Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_drop_df = results_renamed_df.drop(col('statusId'))

# COMMAND ----------

display(results_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write data to datalake as parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_drop_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# race_results = results_drop_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

overwrite_partition(results_drop_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# results_df = results_drop_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/results")

# COMMAND ----------

# spark.read.parquet("/mnt/formulaonedll/processed/results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC; 

# COMMAND ----------

