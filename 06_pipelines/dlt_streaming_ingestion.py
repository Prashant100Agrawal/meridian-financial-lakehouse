# Databricks notebook source
# DBTITLE 1,Streaming Ingestion Pipeline
# MAGIC %md
# MAGIC # Streaming Ingestion Pipeline
# MAGIC ## Real-time data ingestion with Auto Loader
# MAGIC
# MAGIC This pipeline continuously ingests data from multiple sources:
# MAGIC * **Trading Systems**: Real-time trades
# MAGIC * **Market Data**: Price feeds
# MAGIC * **Risk Metrics**: Live risk calculations

# COMMAND ----------

# DBTITLE 1,Import DLT
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Trading Systems Stream
@dlt.table(
    name="trading_systems_stream",
    comment="Real-time trading systems data stream"
)
def trading_systems_stream():
    """Stream trading data from landing zone"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")
        .withColumn("stream_ingestion_time", current_timestamp())
        .withColumn("stream_source", lit("trading_systems"))
    )

# COMMAND ----------

# DBTITLE 1,Market Data Stream
@dlt.table(
    name="market_data_stream",
    comment="Real-time market data feed"
)
def market_data_stream():
    """Stream market data from landing zone"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load("/Volumes/financial_lakehouse/raw/landing_files/market_data")
        .withColumn("stream_ingestion_time", current_timestamp())
        .withColumn("stream_source", lit("market_data"))
    )

# COMMAND ----------

# DBTITLE 1,Risk Metrics Stream
@dlt.table(
    name="risk_metrics_stream",
    comment="Real-time risk metrics feed"
)
def risk_metrics_stream():
    """Stream risk metrics from landing zone"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load("/Volumes/financial_lakehouse/raw/landing_files/risk_metrics")
        .withColumn("stream_ingestion_time", current_timestamp())
        .withColumn("stream_source", lit("risk_metrics"))
    )

# COMMAND ----------

