-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Risk Metrics Analytics
-- MAGIC ## Risk analysis and monitoring

-- COMMAND ----------

-- Large Position Concentration Risk
SELECT 
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END) as net_position,
    SUM(CASE WHEN side = 'BUY' THEN total_value ELSE -total_value END) as net_exposure,
    COUNT(DISTINCT acct_id) as num_accounts
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY symbol
HAVING ABS(SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END)) > 100
ORDER BY ABS(net_exposure) DESC;

-- COMMAND ----------

-- Account Risk Exposure
SELECT 
    acct_id,
    COUNT(DISTINCT symbol) as num_positions,
    SUM(CASE WHEN side = 'BUY' THEN total_value ELSE 0 END) as long_exposure,
    SUM(CASE WHEN side = 'SELL' THEN total_value ELSE 0 END) as short_exposure,
    SUM(CASE WHEN side = 'BUY' THEN total_value ELSE -total_value END) as net_exposure
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY acct_id
ORDER BY ABS(net_exposure) DESC
LIMIT 20;

-- COMMAND ----------

-- Intraday Trading Pattern Analysis
SELECT 
    trade_hour,
    COUNT(*) as trade_count,
    SUM(total_value) as total_volume,
    AVG(total_value) as avg_trade_size,
    COUNT(DISTINCT acct_id) as active_accounts
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY trade_hour
ORDER BY trade_hour;
