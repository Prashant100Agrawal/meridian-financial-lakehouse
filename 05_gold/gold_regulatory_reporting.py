# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Regulatory Reporting
# MAGIC ## Compliance and regulatory reports

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read trading data
df_trades = spark.table("financial_lakehouse.silver.trading_systems_conformed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Large Trade Reporting

# COMMAND ----------

# Identify trades above regulatory threshold
df_large_trades = df_trades.filter(col("total_value") > 50000) \
    .select(
        col("txn_id"),
        col("acct_id"),
        col("symbol"),
        col("side"),
        col("qty"),
        col("unit_price"),
        col("total_value"),
        col("trade_dt"),
        col("trader_id")
    ) \
    .withColumn("report_generated_timestamp", current_timestamp()) \
    .withColumn("regulatory_flag", lit("LARGE_TRADE"))

# COMMAND ----------

df_large_trades.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("financial_lakehouse.gold.regulatory_large_trades")

print(f"✅ Created regulatory large trades report: {df_large_trades.count()} records")
