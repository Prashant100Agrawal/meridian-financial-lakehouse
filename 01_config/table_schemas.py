# Databricks notebook source
# MAGIC %md
# MAGIC # Table Schemas Definition
# MAGIC ## Define all table schemas for the lakehouse

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Trading Systems Schema
trading_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("instrument_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("quantity", DecimalType(18, 4), False),
    StructField("price", DecimalType(18, 4), False),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("currency", StringType(), False),
    StructField("trader_id", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), False)
])

# COMMAND ----------

# Market Data Schema
market_data_schema = StructType([
    StructField("instrument_id", StringType(), False),
    StructField("quote_timestamp", TimestampType(), False),
    StructField("bid_price", DecimalType(18, 4), True),
    StructField("ask_price", DecimalType(18, 4), True),
    StructField("last_price", DecimalType(18, 4), False),
    StructField("volume", LongType(), True),
    StructField("market_cap", DecimalType(24, 2), True),
    StructField("ingestion_timestamp", TimestampType(), False)
])

# COMMAND ----------

# Risk Metrics Schema
risk_metrics_schema = StructType([
    StructField("portfolio_id", StringType(), False),
    StructField("metric_timestamp", TimestampType(), False),
    StructField("var_95", DecimalType(18, 4), True),
    StructField("var_99", DecimalType(18, 4), True),
    StructField("expected_shortfall", DecimalType(18, 4), True),
    StructField("beta", DecimalType(10, 6), True),
    StructField("sharpe_ratio", DecimalType(10, 6), True),
    StructField("ingestion_timestamp", TimestampType(), False)
])
