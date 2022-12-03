# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 Read the JSON file using the spark dataframe Reader API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True),
                                   ])

# COMMAND ----------

drivers_read_df = spark.read.schema(drivers_schema).json(f'/mnt/formulaonedll/raw/{v_file_date}/drivers.json')

# COMMAND ----------

display(drivers_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat

# COMMAND ----------

drivers_final_df = drivers_read_df.withColumnRenamed("driverId", "driver_id") \
                             .withColumnRenamed("driverRef", "driver_ref") \
                             .withColumn("ingestion_date", current_timestamp()) \
                             .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Drop the unwanted Columns

# COMMAND ----------

drivers_drop_df = drivers_final_df.drop(col('url'))

# COMMAND ----------

display(drivers_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write data to datalake as parquet

# COMMAND ----------

drivers_df = drivers_drop_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# drivers_df = drivers_drop_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/drivers")

# COMMAND ----------

# spark.read.parquet("/mnt/formulaonedll/processed/drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

