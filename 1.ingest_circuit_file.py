# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits.csv File

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
# MAGIC #### Step-1 Read the CSV file using spark datframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields= [StructField("circuitId",IntegerType(),False),
                                      StructField("circuitRef",StringType(),True),
                                      StructField("name",StringType(),True),
                                      StructField("location",StringType(),True),
                                      StructField("country",StringType(),True),
                                      StructField("lat",DoubleType(),True),
                                      StructField("lng",DoubleType(),True),
                                      StructField("alt",DoubleType(),True),
                                      StructField("url",StringType(),True)    
])

# COMMAND ----------

circuit_read_df = spark.read.option("header", True).schema(circuits_schema).csv(f"dbfs:{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuit_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename and Select specific columns

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuit_Renamed_df = circuit_read_df.select(col('circuitId').alias('circuit_id'), col('circuitRef').alias('circuit_Ref'), col('name'), col('location'), col('country'), col('lat').alias('latitude'), col('lng').alias('langitude'),col('lng').alias('altiitude')).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuit_Renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Add ingestion Date

# COMMAND ----------

circuit_final_df = add_ingesion_date(circuit_Renamed_df)

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write data to datalake as parquet

# COMMAND ----------

circuits_df = circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# circuits_df = circuit_final_df.write.mode("overwrite").parquet("/mnt/formulaonedll/processed/circuits")

# COMMAND ----------

# spark.read.parquet("/mnt/formulaonedll/processed/circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

