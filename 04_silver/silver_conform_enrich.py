# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Conform & Enrich
# MAGIC ## Standardize schemas and enrich with reference data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read clean silver data
df_clean = spark.table("financial_lakehouse.silver.trading_systems_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conform to Standard Schema

# COMMAND ----------

df_conformed = df_clean.select(
    col("transaction_id").alias("txn_id"),
    col("account_id").alias("acct_id"),
    col("instrument_id").alias("symbol"),
    col("transaction_type").alias("side"),
    col("quantity").alias("qty"),
    col("price").alias("unit_price"),
    (col("quantity") * col("price")).alias("total_value"),
    col("transaction_timestamp").alias("trade_dt"),
    col("currency").alias("ccy"),
    col("trader_id"),
    col("_silver_processed_timestamp").alias("processed_dt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with Calculations

# COMMAND ----------

df_enriched = df_conformed \
    .withColumn("trade_date", to_date(col("trade_dt"))) \
    .withColumn("trade_hour", hour(col("trade_dt"))) \
    .withColumn("is_large_trade", when(col("total_value") > 100000, True).otherwise(False))

# COMMAND ----------

# Write to conformed silver table
target_table = "financial_lakehouse.silver.trading_systems_conformed"

df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

print(f"✅ Written {df_enriched.count()} records to {target_table}")
