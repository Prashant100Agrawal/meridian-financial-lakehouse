-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Portfolio Summary Analytics
-- MAGIC ## Key portfolio metrics and KPIs

-- COMMAND ----------

-- Trading Volume by Instrument
SELECT 
    symbol,
    trade_date,
    SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END) as total_buy_qty,
    SUM(CASE WHEN side = 'SELL' THEN qty ELSE 0 END) as total_sell_qty,
    SUM(total_value) as total_traded_value,
    COUNT(*) as num_trades
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY symbol, trade_date
ORDER BY trade_date DESC, total_traded_value DESC;

-- COMMAND ----------

-- Top Trading Accounts
SELECT 
    acct_id,
    COUNT(*) as total_trades,
    SUM(total_value) as total_value,
    AVG(total_value) as avg_trade_value,
    COUNT(DISTINCT symbol) as num_instruments_traded
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY acct_id
ORDER BY total_value DESC
LIMIT 20;

-- COMMAND ----------

-- Daily Trading Summary
SELECT 
    trade_date,
    COUNT(*) as total_trades,
    COUNT(DISTINCT acct_id) as active_accounts,
    COUNT(DISTINCT symbol) as instruments_traded,
    SUM(total_value) as total_volume,
    AVG(total_value) as avg_trade_size
FROM financial_lakehouse.standardized.trading_systems_conformed
GROUP BY trade_date
ORDER BY trade_date DESC;
