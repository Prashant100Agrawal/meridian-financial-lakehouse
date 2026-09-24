# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Log Queries
# MAGIC ## Monitor data access and compliance using Unity Catalog audit logs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Coverage
# MAGIC * Data access patterns
# MAGIC * Privilege changes
# MAGIC * Failed access attempts
# MAGIC * PII column access
# MAGIC * Anomaly detection

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Recent Data Access Events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show recent table access in last 7 days
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   event_time,
# MAGIC   user_identity.email as user_email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg as table_accessed,
# MAGIC   response.status_code
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   AND action_name IN ('getTable', 'listTables', 'getTableData')
# MAGIC   AND request_params.full_name_arg LIKE 'financial_lakehouse.%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Failed Access Attempts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify failed access attempts (potential security issues)
# MAGIC SELECT 
# MAGIC   DATE(event_time) as date,
# MAGIC   user_identity.email as user_email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg as resource,
# MAGIC   response.error_message,
# MAGIC   COUNT(*) as failure_count
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   AND response.status_code >= 400
# MAGIC   AND request_params.full_name_arg LIKE 'financial_lakehouse.%'
# MAGIC GROUP BY date, user_email, action_name, resource, response.error_message
# MAGIC HAVING failure_count > 3
# MAGIC ORDER BY failure_count DESC, date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PII Column Access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track access to PII columns
# MAGIC SELECT 
# MAGIC   DATE(event_time) as access_date,
# MAGIC   user_identity.email as user_email,
# MAGIC   request_params.full_name_arg as table_name,
# MAGIC   COUNT(*) as access_count
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   AND action_name = 'getTableData'
# MAGIC   AND request_params.full_name_arg IN (
# MAGIC     -- Tables with PII
# MAGIC     SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name)
# MAGIC     FROM system.information_schema.table_tags
# MAGIC     WHERE tag_name = 'pii_level' 
# MAGIC       AND tag_value IN ('High', 'Medium')
# MAGIC   )
# MAGIC GROUP BY access_date, user_email, table_name
# MAGIC ORDER BY access_date DESC, access_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Privilege Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track privilege grants and revokes
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   event_time,
# MAGIC   user_identity.email as admin_user,
# MAGIC   action_name,
# MAGIC   request_params.securable_type,
# MAGIC   request_params.securable_full_name,
# MAGIC   request_params.principal as granted_to,
# MAGIC   request_params.privileges
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC   AND action_name IN ('createGrant', 'revokeGrant', 'updateGrant')
# MAGIC   AND request_params.securable_full_name LIKE 'financial_lakehouse.%'
# MAGIC ORDER BY event_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top Users by Access Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify most active users accessing financial data
# MAGIC SELECT 
# MAGIC   user_identity.email as user_email,
# MAGIC   COUNT(DISTINCT DATE(event_time)) as active_days,
# MAGIC   COUNT(*) as total_queries,
# MAGIC   COUNT(DISTINCT request_params.full_name_arg) as unique_tables
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   AND action_name = 'getTableData'
# MAGIC   AND request_params.full_name_arg LIKE 'financial_lakehouse.%'
# MAGIC GROUP BY user_email
# MAGIC ORDER BY total_queries DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Access Pattern Anomalies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detect unusual access patterns (high volume from single user)
# MAGIC WITH user_baselines AS (
# MAGIC   SELECT 
# MAGIC     user_identity.email as user_email,
# MAGIC     AVG(daily_count) as avg_daily_access,
# MAGIC     STDDEV(daily_count) as stddev_daily_access
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       DATE(event_time) as date,
# MAGIC       user_identity.email,
# MAGIC       COUNT(*) as daily_count
# MAGIC     FROM system.access.audit
# MAGIC     WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC       AND action_name = 'getTableData'
# MAGIC     GROUP BY date, user_identity.email
# MAGIC   )
# MAGIC   GROUP BY user_email
# MAGIC )
# MAGIC SELECT 
# MAGIC   DATE(a.event_time) as date,
# MAGIC   a.user_identity.email as user_email,
# MAGIC   COUNT(*) as access_count,
# MAGIC   b.avg_daily_access,
# MAGIC   ROUND((COUNT(*) - b.avg_daily_access) / NULLIF(b.stddev_daily_access, 0), 2) as std_dev_from_mean
# MAGIC FROM system.access.audit a
# MAGIC JOIN user_baselines b ON a.user_identity.email = b.user_email
# MAGIC WHERE a.event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   AND a.action_name = 'getTableData'
# MAGIC GROUP BY date, user_email, avg_daily_access, stddev_daily_access
# MAGIC HAVING std_dev_from_mean > 3  -- More than 3 standard deviations
# MAGIC ORDER BY std_dev_from_mean DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Compliance Report: Data Access by Business Unit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track which business units are accessing which data domains
# MAGIC SELECT 
# MAGIC   DATE(event_time) as access_date,
# MAGIC   SUBSTRING_INDEX(user_identity.email, '@', -1) as organization,
# MAGIC   request_params.full_name_arg as table_name,
# MAGIC   COUNT(*) as access_count
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   AND action_name = 'getTableData'
# MAGIC   AND request_params.full_name_arg LIKE 'financial_lakehouse.gold.%'
# MAGIC GROUP BY access_date, organization, table_name
# MAGIC ORDER BY access_date DESC, access_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Export Audit Report

# COMMAND ----------

# Export compliance audit report
audit_df = spark.sql("""
  SELECT 
    event_date,
    user_identity.email as user_email,
    action_name,
    request_params.full_name_arg as resource_accessed,
    response.status_code,
    response.error_message
  FROM system.access.audit
  WHERE event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
    AND request_params.full_name_arg LIKE 'financial_lakehouse.%'
  ORDER BY event_date DESC, event_time DESC
""")

# Save to Delta table for long-term storage
audit_df.write.mode("overwrite").saveAsTable("financial_lakehouse.gold.audit_log_summary")

print("✅ Audit report exported to financial_lakehouse.gold.audit_log_summary")

# COMMAND ----------

print("✅ Audit log queries complete")
