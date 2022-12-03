# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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
# MAGIC #### Step-1 Read the json file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_read_df = spark.read.schema(constructors_schema).json(f'dbfs:{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(constructors_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Drop Unwanted Columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_drop_df = constructors_read_df.drop(col('url'))

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_Ref").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write data to datalake as parquet

# COMMAND ----------

constructors_df = constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# constructors_df = constructor_final_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/constructors")

# COMMAND ----------

# spark.read.parquet("/mnt/formulaonedll/processed/constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

