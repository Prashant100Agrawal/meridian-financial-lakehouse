# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Streaming Ingestion
# MAGIC ## Continuous ingestion using Auto Loader

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Auto Loader for Trading Systems

# COMMAND ----------

# Define source and target
source_path = "/Volumes/financial_lakehouse/raw/landing_files/trading_systems"
checkpoint_path = "/Volumes/financial_lakehouse/raw/checkpoints/trading_systems"
target_table = "financial_lakehouse.raw.trading_systems_stream"

# COMMAND ----------

# Read streaming data with Auto Loader
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .load(source_path)

# COMMAND ----------

# Write to Delta table
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(target_table)

# COMMAND ----------

# Display streaming status
display(query.status)