# Databricks notebook source
# DBTITLE 1,Silver Clean & Dedup
# MAGIC %md
# MAGIC # Silver Layer - Clean & Deduplicate
# MAGIC ## Read from bronze, remove duplicates, validate data quality

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read Bronze
# Read from bronze
df_bronze = spark.table("financial_lakehouse.bronze.trading_systems")
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------

# DBTITLE 1,Deduplicate
# Deduplicate by transaction_id (keep latest ingestion)
w = Window.partitionBy("transaction_id").orderBy(col("_bronze_ingestion_timestamp").desc())
df_deduped = df_bronze \
    .withColumn("_row_num", row_number().over(w)) \
    .filter(col("_row_num") == 1) \
    .drop("_row_num")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# Data quality: drop rows with null critical fields + filter invalid values
df_clean = df_deduped \
    .filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("instrument_id").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("transaction_type").isNotNull()
    ) \
    .filter(
        (col("price") > 0) &
        (col("quantity") > 0) &
        (col("transaction_type").isin("BUY", "SELL"))
    )

# COMMAND ----------

# DBTITLE 1,Write Silver Clean
# Add silver processing metadata and write
df_clean = df_clean.withColumn("_silver_processed_timestamp", current_timestamp())

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("financial_lakehouse.silver.trading_systems_clean")

print(f"✅ Written to silver.trading_systems_clean")

# COMMAND ----------

