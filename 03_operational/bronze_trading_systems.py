# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Trading Systems
# MAGIC ## Load raw data into bronze tables (immutable)

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# Use glob pattern to read all batch subdirectories
raw_path = "/Volumes/financial_lakehouse/raw/landing_files/trading_systems/*/*.parquet"
bronze_table = "financial_lakehouse.operational.trading_systems"

# COMMAND ----------

# Read from raw
df_raw = spark.read.parquet(raw_path)

# COMMAND ----------

# Add bronze layer metadata (use _metadata.file_path - UC compatible)
df_bronze = df_raw \
    .withColumn("_bronze_ingestion_timestamp", current_timestamp()) \
    .withColumn("_bronze_file_path", col("_metadata.file_path"))

# COMMAND ----------

# Write to bronze table (append mode - immutable)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(bronze_table)

# COMMAND ----------

print(f"✅ Loaded {df_bronze.count()} records into {bronze_table}")

# COMMAND ----------

# Verify data
display(spark.table(bronze_table).limit(10))