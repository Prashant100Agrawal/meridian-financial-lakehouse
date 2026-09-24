# Databricks notebook source
# DBTITLE 1,Lakehouse 1: Enterprise Data Hub - Medallion Pipeline
# MAGIC %md
# MAGIC # Lakehouse 1: Enterprise Data Hub - Medallion Pipeline
# MAGIC ## Bronze → Silver → Gold transformations using Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC This pipeline implements the medallion architecture:
# MAGIC * **Bronze**: Raw, immutable ingestion
# MAGIC * **Silver**: Cleansed, conformed, deduplicated
# MAGIC * **Gold**: Business-ready aggregates and dimensions

# COMMAND ----------

# DBTITLE 1,Import DLT
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Bronze - Trading Systems
@dlt.table(
    name="bronze_trading_systems",
    comment="Raw trading systems data with full history",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "transaction_id"
    }
)
def bronze_trading_systems():
    """Ingest raw trading data from landing zone"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")
        .withColumn("bronze_ingest_timestamp", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Silver - Cleansed Trading
@dlt.table(
    name="silver_trading_clean",
    comment="Cleansed and validated trading data"
)
@dlt.expect_all_or_drop({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_quantity": "quantity > 0",
    "valid_price": "price > 0"
})
def silver_trading_clean():
    """Clean and validate bronze trading data"""
    return (
        dlt.read_stream("bronze_trading_systems")
        .dropDuplicates(["transaction_id"])
        .withColumn("silver_processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Silver - Conformed Trading
@dlt.table(
    name="silver_trading_conformed",
    comment="Standardized trading data with business rules applied"
)
def silver_trading_conformed():
    """Apply business rules and standardization"""
    return (
        dlt.read("silver_trading_clean")
        .withColumn("txn_id", col("transaction_id"))
        .withColumn("acct_id", col("account_id"))
        .withColumn("symbol", col("instrument_id"))
        .withColumn("side", col("transaction_type"))
        .withColumn("qty", col("quantity"))
        .withColumn("unit_price", col("price"))
        .withColumn("total_value", col("quantity") * col("price"))
        .withColumn("trade_dt", col("transaction_timestamp"))
        .withColumn("ccy", col("currency"))
        .withColumn("processed_dt", current_timestamp())
        .withColumn("trade_date", to_date(col("transaction_timestamp")))
        .withColumn("trade_hour", hour(col("transaction_timestamp")))
        .withColumn("is_large_trade", when(col("total_value") > 100000, True).otherwise(False))
        .select(
            "txn_id", "acct_id", "symbol", "side", "qty", "unit_price",
            "total_value", "trade_dt", "ccy", "trader_id", "processed_dt",
            "trade_date", "trade_hour", "is_large_trade"
        )
    )

# COMMAND ----------

# DBTITLE 1,Gold - Daily Trading Summary
@dlt.table(
    name="gold_daily_trading_summary",
    comment="Daily trading metrics aggregated by account and instrument"
)
def gold_daily_trading_summary():
    """Aggregate daily trading metrics"""
    return (
        dlt.read("silver_trading_conformed")
        .groupBy("trade_date", "acct_id", "symbol", "ccy")
        .agg(
            count("*").alias("trade_count"),
            sum(when(col("side") == "BUY", col("qty")).otherwise(0)).alias("buy_volume"),
            sum(when(col("side") == "SELL", col("qty")).otherwise(0)).alias("sell_volume"),
            sum(when(col("side") == "BUY", col("total_value")).otherwise(0)).alias("buy_value"),
            sum(when(col("side") == "SELL", col("total_value")).otherwise(0)).alias("sell_value"),
            avg("unit_price").alias("avg_price"),
            min("unit_price").alias("min_price"),
            max("unit_price").alias("max_price")
        )
        .withColumn("net_position", col("buy_volume") - col("sell_volume"))
        .withColumn("net_value", col("sell_value") - col("buy_value"))
        .withColumn("calculated_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Gold - Account Dimensions
@dlt.table(
    name="gold_dim_accounts",
    comment="Account dimension table"
)
def gold_dim_accounts():
    """Create account dimension"""
    return (
        dlt.read("silver_trading_conformed")
        .select("acct_id", "ccy")
        .distinct()
        .withColumn("account_status", lit("ACTIVE"))
        .withColumn("account_type", lit("TRADING"))
        .withColumn("effective_date", current_date())
        .withColumn("dim_created_at", current_timestamp())
    )

# COMMAND ----------

