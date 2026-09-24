# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - External APIs
# MAGIC ## Load market and regulatory API data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

raw_path = "/Volumes/financial_lakehouse/raw/landing_files/market_data"
bronze_table = "financial_lakehouse.operational.market_data"

# COMMAND ----------

df_raw = spark.read.parquet(raw_path)

df_bronze = df_raw \
    .withColumn("_bronze_ingestion_timestamp", current_timestamp()) \
    .withColumn("_bronze_file_path", input_file_name())

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(bronze_table)

print(f"✅ Loaded {df_bronze.count()} records into {bronze_table}")
