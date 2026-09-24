# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Financial Analytics
# MAGIC ## Business-ready analytics aggregates

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Read from silver
df_silver = spark.table("financial_lakehouse.standardized.trading_systems_conformed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Trading Summary

# COMMAND ----------

df_daily_summary = df_silver.groupBy("trade_date", "symbol", "side") \
    .agg(
        count("*").alias("num_trades"),
        sum("qty").alias("total_quantity"),
        sum("total_value").alias("total_value"),
        avg("unit_price").alias("avg_price"),
        min("unit_price").alias("min_price"),
        max("unit_price").alias("max_price")
    ) \
    .withColumn("created_timestamp", current_timestamp())

# COMMAND ----------

# Write to gold table
df_daily_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("financial_lakehouse.reporting.daily_trading_summary")

print(f"✅ Created daily trading summary: {df_daily_summary.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account Performance

# COMMAND ----------

df_account_perf = df_silver.groupBy("acct_id", "trade_date") \
    .agg(
        count("*").alias("num_trades"),
        sum("total_value").alias("total_traded_value"),
        countDistinct("symbol").alias("num_instruments"),
        sum(when(col("side") == "BUY", col("total_value")).otherwise(0)).alias("buy_value"),
        sum(when(col("side") == "SELL", col("total_value")).otherwise(0)).alias("sell_value")
    ) \
    .withColumn("net_position", col("buy_value") - col("sell_value")) \
    .withColumn("created_timestamp", current_timestamp())

# COMMAND ----------

df_account_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("financial_lakehouse.reporting.account_performance")

print(f"✅ Created account performance: {df_account_perf.count()} records")
