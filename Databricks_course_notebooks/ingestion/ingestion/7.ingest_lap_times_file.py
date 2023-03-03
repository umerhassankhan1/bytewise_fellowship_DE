# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("position", StringType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)                                                                                    
                                       ])

# COMMAND ----------

lap_times_read_df = spark.read.schema(lap_times_schema).option("multiLine", True).csv(f'/mnt/formulaonedll/raw/{v_file_date}/lap_times')

# COMMAND ----------

lap_times_read_df.count()

# COMMAND ----------

display(lap_times_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_renamed_df = lap_times_read_df.withColumnRenamed("raceId", "race_id")\
                                      .withColumnRenamed("driverId", "driver_id")\
                                      .withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write data to datalake as parquet

# COMMAND ----------

laptimes_df = lap_times_renamed_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/laptimes")

# COMMAND ----------

overwrite_partition(lap_times_renamed_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

