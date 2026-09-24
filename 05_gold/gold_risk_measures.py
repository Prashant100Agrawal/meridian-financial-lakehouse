# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Risk Measures
# MAGIC ## Risk metrics and compliance

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read risk metrics from bronze
df_risk = spark.table("financial_lakehouse.bronze.risk_metrics") if spark.catalog.tableExists("financial_lakehouse.bronze.risk_metrics") else None

if df_risk:
    # Aggregate risk metrics
    df_risk_summary = df_risk.groupBy("portfolio_id", to_date("metric_timestamp").alias("date")) \
        .agg(
            avg("var_95").alias("avg_var_95"),
            avg("var_99").alias("avg_var_99"),
            avg("expected_shortfall").alias("avg_expected_shortfall"),
            avg("beta").alias("avg_beta"),
            avg("sharpe_ratio").alias("avg_sharpe_ratio")
        ) \
        .withColumn("created_timestamp", current_timestamp())
    
    df_risk_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("financial_lakehouse.gold.risk_summary")
    
    print(f"✅ Created risk summary: {df_risk_summary.count()} records")
else:
    print("⚠️ Risk metrics table not found")
