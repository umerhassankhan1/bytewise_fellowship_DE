# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC #### Step-1 Read the csv file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                 ])

# COMMAND ----------

races_read_df = spark.read.option("header", True).schema(races_schema).csv(f"dbfs:{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Add ingestion date and race time stamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

races_with_timestamp = add_ingesion_date(races_read_df).withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(races_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Select only the columns required

# COMMAND ----------

races_selected_df = races_with_timestamp.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('date'), col('time'), col('url'), col('ingestion_date'), col('race_timestamp'), col('data_source'), col('file_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write data to datalake as parquet

# COMMAND ----------

races_df= races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# races_df = races_selected_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/races")

# COMMAND ----------

# spark.read.parquet("/mnt/formulaonedll/processed/races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

