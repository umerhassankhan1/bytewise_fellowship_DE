# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

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

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), True),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(),True)
                                       ])

# COMMAND ----------

qualifying_read_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f'/mnt/formulaonedll/raw/{v_file_date}/qualifying')

# COMMAND ----------

qualifying_read_df.count()

# COMMAND ----------

display(qualifying_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_read_df.withColumnRenamed("qualifyId", "qualifying_id")\
                                          .withColumnRenamed("raceId", "race_id")\
                                          .withColumnRenamed("driverId", "driver_id")\
                                          .withColumnRenamed("constructorId", "constructor_id")\
                                          .withColumn("ingestion_date", current_timestamp())\
                                          .withColumn("data_source", lit(v_data_source))\
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write data to datalake as parquet

# COMMAND ----------

overwrite_partition(qualifying_renamed_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

qualifying_df = qualifying_renamed_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

