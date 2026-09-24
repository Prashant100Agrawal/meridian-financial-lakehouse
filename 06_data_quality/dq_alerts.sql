-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Quality Alerts
-- MAGIC ## SQL queries for DQ alerting

-- COMMAND ----------

-- Check for data freshness
SELECT 
    'trading_systems' as table_name,
    MAX(trade_dt) as latest_trade_date,
    DATEDIFF(CURRENT_DATE(), MAX(trade_dt)) as days_since_last_trade,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), MAX(trade_dt)) > 1 THEN 'ALERT'
        ELSE 'OK'
    END as freshness_status
FROM financial_lakehouse.silver.trading_systems_conformed;

-- COMMAND ----------

-- Check for record count anomalies
SELECT 
    trade_date,
    COUNT(*) as record_count,
    AVG(COUNT(*)) OVER (ORDER BY trade_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as avg_7day_count,
    CASE 
        WHEN COUNT(*) < 0.5 * AVG(COUNT(*)) OVER (ORDER BY trade_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) THEN 'LOW_VOLUME_ALERT'
        WHEN COUNT(*) > 2 * AVG(COUNT(*)) OVER (ORDER BY trade_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) THEN 'HIGH_VOLUME_ALERT'
        ELSE 'OK'
    END as volume_status
FROM financial_lakehouse.silver.trading_systems_conformed
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT 30;
