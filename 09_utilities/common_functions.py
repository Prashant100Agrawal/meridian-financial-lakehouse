# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utility Functions
# MAGIC ## Reusable functions across the project

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

def add_audit_columns(df):
    """Add standard audit columns to dataframe"""
    return df \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("created_by", lit("system")) \
        .withColumn("updated_timestamp", current_timestamp()) \
        .withColumn("updated_by", lit("system"))

# COMMAND ----------

def get_latest_partition(table_name, partition_col="trade_date"):
    """Get the latest partition value from a table"""
    df = spark.table(table_name)
    latest = df.agg(max(partition_col)).collect()[0][0]
    return latest

# COMMAND ----------

def table_exists(catalog, schema, table):
    """Check if a table exists"""
    full_name = f"{catalog}.{schema}.{table}"
    return spark.catalog.tableExists(full_name)

# COMMAND ----------

def log_pipeline_run(pipeline_name, status, record_count=None, error_msg=None):
    """Log pipeline execution"""
    log_entry = spark.createDataFrame([{
        'pipeline_name': pipeline_name,
        'run_timestamp': datetime.now(),
        'status': status,
        'record_count': record_count,
        'error_message': error_msg
    }])
    
    log_entry.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("financial_lakehouse.gold.pipeline_run_log")
