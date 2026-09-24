# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleanse & Deduplicate
# MAGIC ## Clean and deduplicate bronze data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Read from bronze
bronze_table = "financial_lakehouse.bronze.trading_systems"
df_bronze = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing

# COMMAND ----------

# Remove nulls in critical fields
df_clean = df_bronze.filter(
    col("transaction_id").isNotNull() &
    col("account_id").isNotNull() &
    col("instrument_id").isNotNull()
)

# Standardize data types and formats
df_clean = df_clean \
    .withColumn("instrument_id", upper(trim(col("instrument_id")))) \
    .withColumn("transaction_type", upper(trim(col("transaction_type")))) \
    .withColumn("currency", upper(trim(col("currency"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

# Define window for deduplication (keep latest record)
window_spec = Window.partitionBy("transaction_id").orderBy(col("_bronze_ingestion_timestamp").desc())

df_deduped = df_clean \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# COMMAND ----------

# Add silver layer metadata
df_silver = df_deduped \
    .withColumn("_silver_processed_timestamp", current_timestamp()) \
    .withColumn("_data_quality_flag", lit("PASSED"))

# COMMAND ----------

# Write to silver table
silver_table = "financial_lakehouse.silver.trading_systems_clean"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

# COMMAND ----------

print(f"✅ Bronze records: {df_bronze.count()}")
print(f"✅ After cleansing: {df_clean.count()}")
print(f"✅ After deduplication: {df_deduped.count()}")
print(f"✅ Written to: {silver_table}")
