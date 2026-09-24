-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Compliance Reports
-- MAGIC ## Regulatory and compliance analytics

-- COMMAND ----------

-- Large Trade Reporting (>$50K threshold)
SELECT 
    txn_id,
    acct_id,
    symbol,
    side,
    qty,
    unit_price,
    total_value,
    trade_dt,
    trader_id
FROM financial_lakehouse.silver.trading_systems_conformed
WHERE total_value > 50000
ORDER BY total_value DESC;

-- COMMAND ----------

-- Suspicious Trading Pattern Detection
-- Multiple large trades in short time window
WITH trade_windows AS (
    SELECT 
        acct_id,
        symbol,
        COUNT(*) as num_trades,
        SUM(total_value) as total_value,
        MIN(trade_dt) as first_trade,
        MAX(trade_dt) as last_trade,
        (UNIX_TIMESTAMP(MAX(trade_dt)) - UNIX_TIMESTAMP(MIN(trade_dt))) / 60 as time_window_minutes
    FROM financial_lakehouse.silver.trading_systems_conformed
    WHERE trade_date = CURRENT_DATE()
    GROUP BY acct_id, symbol, DATE_TRUNC('HOUR', trade_dt)
)
SELECT *
FROM trade_windows
WHERE num_trades >= 5 
  AND time_window_minutes <= 15
ORDER BY total_value DESC;

-- COMMAND ----------

-- Trade Audit Trail
SELECT 
    txn_id,
    acct_id,
    trader_id,
    symbol,
    side,
    qty,
    unit_price,
    total_value,
    trade_dt,
    processed_dt
FROM financial_lakehouse.silver.trading_systems_conformed
ORDER BY trade_dt DESC
LIMIT 100;