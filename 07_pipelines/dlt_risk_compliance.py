# Databricks notebook source
# DBTITLE 1,Lakehouse 3: Risk & Compliance Pipeline
# MAGIC %md
# MAGIC # Lakehouse 3: Risk & Compliance Pipeline
# MAGIC ## Risk Measures, Regulatory Reporting, and Audit
# MAGIC
# MAGIC This pipeline populates the risk_compliance catalog with:
# MAGIC * **VaR Calculations**: Value at Risk (95%, 99%)
# MAGIC * **Regulatory Reports**: Large trade monitoring
# MAGIC * **Audit Trails**: Processing logs and compliance tracking

# COMMAND ----------

# DBTITLE 1,Import DLT
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,VaR Calculations
@dlt.table(
    name="var_calculations",
    comment="Value at Risk calculations with confidence intervals"
)
def var_calculations():
    """Calculate VaR and risk metrics"""
    # Read P&L from financial_analytics
    return (
        spark.sql("""
            WITH daily_returns AS (
                SELECT 
                    acct_id,
                    trade_date,
                    net_pnl as daily_return,
                    currency
                FROM financial_analytics.nav_pnl.profit_loss
                WHERE net_pnl IS NOT NULL
            ),
            portfolio_stats AS (
                SELECT 
                    acct_id,
                    currency,
                    AVG(daily_return) as mean_return,
                    STDDEV(daily_return) as std_dev,
                    COUNT(*) as observation_days,
                    MIN(daily_return) as worst_loss,
                    MAX(daily_return) as best_gain
                FROM daily_returns
                GROUP BY acct_id, currency
            )
            SELECT 
                acct_id,
                currency,
                mean_return,
                std_dev,
                observation_days,
                mean_return - (1.645 * std_dev) as var_95,
                mean_return - (2.326 * std_dev) as var_99,
                mean_return - (2.5 * std_dev) as expected_shortfall,
                worst_loss,
                best_gain,
                CASE 
                    WHEN std_dev > 0 THEN mean_return / std_dev 
                    ELSE 0 
                END as sharpe_ratio,
                CURRENT_TIMESTAMP() as calculated_at
            FROM portfolio_stats
        """)
    )

# COMMAND ----------

# DBTITLE 1,Large Trade Report
@dlt.table(
    name="large_trade_report",
    comment="Regulatory reporting for large trades"
)
def large_trade_report():
    """Monitor large trades for regulatory compliance"""
    trading_df = spark.table("financial_lakehouse.standardized.trading_systems_conformed")
    
    return (
        trading_df
        .filter(col("trade_date") >= date_sub(current_date(), 30))  # Last 30 days
        .withColumn(
            "regulatory_category",
            when(col("total_value") > 100000, lit("HIGH_VALUE"))
            .when(col("total_value") > 50000, lit("MEDIUM_VALUE"))
            .otherwise(lit("STANDARD"))
        )
        .withColumn(
            "compliance_status",
            when((col("is_large_trade") == True) & (col("total_value") > 100000), lit("REQUIRES_REVIEW"))
            .otherwise(lit("AUTO_APPROVED"))
        )
        .withColumn("report_timestamp", current_timestamp())
        .select(
            "txn_id", "acct_id", "trader_id", "symbol", "side",
            "qty", "unit_price", "total_value", "trade_dt", "trade_date",
            "ccy", "is_large_trade", "regulatory_category", "compliance_status",
            "report_timestamp"
        )
    )

# COMMAND ----------

# DBTITLE 1,Audit Trail
@dlt.table(
    name="trade_audit_trail",
    comment="Complete audit trail for compliance monitoring"
)
def trade_audit_trail():
    """Create audit trail for all trades"""
    trading_df = spark.table("financial_lakehouse.standardized.trading_systems_conformed")
    
    return (
        trading_df
        .withColumn("data_layer", lit("SILVER"))
        .withColumn("source_system", lit("TRADING_SYSTEM"))
        .withColumnRenamed("ccy", "transaction_currency")
        .withColumn(
            "processing_lag_seconds",
            (unix_timestamp(col("processed_dt")) - unix_timestamp(col("trade_dt")))
        )
        .withColumn(
            "processing_status",
            when(col("processing_lag_seconds") > 60, lit("DELAYED"))
            .otherwise(lit("ON_TIME"))
        )
        .withColumn("audit_timestamp", current_timestamp())
        .select(
            "txn_id", "acct_id", "trader_id", "symbol", "side", "total_value",
            col("trade_dt").alias("transaction_timestamp"),
            col("processed_dt").alias("system_processed_timestamp"),
            "data_layer", "source_system", "transaction_currency",
            "processing_lag_seconds", "processing_status", "audit_timestamp"
        )
    )

# COMMAND ----------

