# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pit-stops.json file

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
# MAGIC #### Step-1 Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType, FloatType

# COMMAND ----------

pitstops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("stop", StringType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("duration", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)                                                                                    
                                       ])

# COMMAND ----------

pitstops_read_df = spark.read.schema(pitstops_schema).option("multiLine", True).json(f'/mnt/formulaonedll/raw/{v_file_date}/pit_stops.json')

# COMMAND ----------

display(pitstops_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_renamed_df = pitstops_read_df.withColumnRenamed("raceId", "race_id")\
                                      .withColumnRenamed("driverId", "driver_id")\
                                      .withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write data to datalake as parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.pitstops;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in pitstops_renamed_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.pitstops")):
#         spark.sql(f"ALTER TABLE f1_processed.pitstops DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# pitstops_renamed_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

overwrite_partition(pitstops_renamed_df, "f1_processed", "pitstops", "race_id")

# COMMAND ----------

# pitstops_df = pitstops_renamed_df.write.mode("overwrite").parquet(f"/mnt/formulaonedll/processed/pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

