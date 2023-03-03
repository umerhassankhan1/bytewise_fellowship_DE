# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/formulaonedll/processed/circuits.csv").filter("circuit_id<70").withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet("/mnt/formulaonedll/processed/races.csv").filter("race_year=2019").withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "inner").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outer Join

# COMMAND ----------

# left outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "left").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "right").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

 # Full outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "full").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuit_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

