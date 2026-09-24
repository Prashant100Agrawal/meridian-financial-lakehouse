# Databricks notebook source
# DBTITLE 1,Lakehouse 2: Financial Analytics Pipeline
# MAGIC %md
# MAGIC # Lakehouse 2: Financial Analytics Pipeline
# MAGIC ## NAV, P&L, and Portfolio Performance
# MAGIC
# MAGIC This pipeline populates the financial_analytics catalog with:
# MAGIC * **NAV Calculations**: Net Asset Value by account
# MAGIC * **P&L**: Profit & Loss analysis
# MAGIC * **Portfolio Metrics**: Performance, risk-adjusted returns

# COMMAND ----------

# DBTITLE 1,Import DLT
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,NAV Calculations
@dlt.table(
    name="daily_nav",
    comment="Daily Net Asset Value calculations per account",
    table_properties={
        "quality": "gold"
    }
)
def daily_nav():
    """Calculate daily NAV from trading data"""
    # Read from Lakehouse 1 silver layer
    trading_df = spark.table("financial_lakehouse.silver.trading_systems_conformed")
    
    return (
        trading_df
        .groupBy("acct_id", "trade_date", "ccy")
        .agg(
            sum(
                when(col("side") == "BUY", col("total_value"))
                .when(col("side") == "SELL", -col("total_value"))
                .otherwise(0)
            ).alias("net_position_value"),
            countDistinct("symbol").alias("unique_instruments"),
            count("*").alias("transaction_count"),
            sum("qty").alias("total_quantity")
        )
        .withColumnRenamed("trade_date", "calculation_date")
        .withColumnRenamed("ccy", "currency")
        .withColumn("calculated_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,P&L Analysis
@dlt.table(
    name="profit_loss",
    comment="Profit & Loss analysis by account, date, and instrument"
)
def profit_loss():
    """Calculate P&L metrics"""
    trading_df = spark.table("financial_lakehouse.silver.trading_systems_conformed")
    
    return (
        trading_df
        .groupBy("acct_id", "trade_date", "symbol", "ccy")
        .agg(
            # Revenue from sells
            sum(when(col("side") == "SELL", col("total_value")).otherwise(0)).alias("total_revenue"),
            # Cost from buys
            sum(when(col("side") == "BUY", col("total_value")).otherwise(0)).alias("total_cost"),
            # Net P&L
            sum(
                when(col("side") == "SELL", col("total_value"))
                .when(col("side") == "BUY", -col("total_value"))
                .otherwise(0)
            ).alias("net_pnl"),
            # Volumes
            sum(when(col("side") == "BUY", col("qty")).otherwise(0)).alias("buy_volume"),
            sum(when(col("side") == "SELL", col("qty")).otherwise(0)).alias("sell_volume"),
            # Counts
            count(when(col("side") == "BUY", 1)).alias("buy_count"),
            count(when(col("side") == "SELL", 1)).alias("sell_count")
        )
        .withColumnRenamed("ccy", "currency")
        .withColumn("calculated_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Portfolio Performance
@dlt.table(
    name="account_performance",
    comment="Account-level performance metrics and risk-adjusted returns"
)
def account_performance():
    """Calculate portfolio performance metrics"""
    # Read from our own P&L table
    return (
        spark.sql("""
            SELECT 
                acct_id,
                currency,
                SUM(net_pnl) as total_pnl,
                AVG(net_pnl) as avg_daily_pnl,
                STDDEV(net_pnl) as pnl_volatility,
                CASE 
                    WHEN STDDEV(net_pnl) > 0 THEN AVG(net_pnl) / STDDEV(net_pnl)
                    ELSE 0 
                END as sharpe_ratio,
                SUM(buy_count + sell_count) as total_trades,
                SUM(buy_volume + sell_volume) as total_volume,
                SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as winning_days,
                SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END) as losing_days,
                COUNT(DISTINCT symbol) as instruments_traded,
                COUNT(DISTINCT trade_date) as trading_days,
                CURRENT_TIMESTAMP() as calculated_at
            FROM financial_analytics.live.profit_loss
            GROUP BY acct_id, currency
        """)
    )

# COMMAND ----------

