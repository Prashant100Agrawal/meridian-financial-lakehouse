# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Monitoring
# MAGIC ## Monitor and track data quality metrics over time

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

def log_dq_metrics(table_name, metric_name, metric_value, status):
    """Log data quality metrics"""
    
    dq_log = spark.createDataFrame([{
        'table_name': table_name,
        'metric_name': metric_name,
        'metric_value': metric_value,
        'status': status,
        'check_timestamp': datetime.now()
    }])
    
    dq_log.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("financial_lakehouse.gold.dq_metrics_log")
    
    print(f"✅ Logged DQ metric: {metric_name} = {metric_value}")

# COMMAND ----------

# Example: Log metrics
log_dq_metrics(
    table_name="financial_lakehouse.silver.trading_systems_conformed",
    metric_name="record_count",
    metric_value=1000,
    status="PASS"
)
