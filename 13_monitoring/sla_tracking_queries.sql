# Databricks notebook source
# MAGIC %md
# MAGIC # SLA Tracking Queries
# MAGIC ## Monitor pipeline execution and data freshness SLAs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SLA Definitions
# MAGIC * **Pipeline Execution**: Bronze/Silver/Gold pipelines complete within 1 hour
# MAGIC * **Data Freshness**: Gold tables updated within 2 hours of source data
# MAGIC * **Query Performance**: P95 query latency < 5 seconds
# MAGIC * **Availability**: 99.9% uptime for gold layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Execution SLA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track pipeline run durations and SLA violations
# MAGIC WITH pipeline_runs AS (
# MAGIC   SELECT 
# MAGIC     pipeline_name,
# MAGIC     update_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
# MAGIC     status,
# MAGIC     CASE 
# MAGIC       WHEN status = 'COMPLETED' AND TIMESTAMPDIFF(MINUTE, start_time, end_time) <= 60 THEN 'Met SLA'
# MAGIC       WHEN status = 'COMPLETED' AND TIMESTAMPDIFF(MINUTE, start_time, end_time) > 60 THEN 'SLA Violation'
# MAGIC       WHEN status = 'FAILED' THEN 'Failed'
# MAGIC       ELSE 'Running'
# MAGIC     END as sla_status
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC     AND event_type = 'update'
# MAGIC )
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   DATE(start_time) as run_date,
# MAGIC   COUNT(*) as total_runs,
# MAGIC   SUM(CASE WHEN sla_status = 'Met SLA' THEN 1 ELSE 0 END) as sla_met_count,
# MAGIC   SUM(CASE WHEN sla_status = 'SLA Violation' THEN 1 ELSE 0 END) as sla_violation_count,
# MAGIC   SUM(CASE WHEN sla_status = 'Failed' THEN 1 ELSE 0 END) as failed_count,
# MAGIC   ROUND(AVG(duration_minutes), 1) as avg_duration_min,
# MAGIC   ROUND(MAX(duration_minutes), 1) as max_duration_min,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN sla_status = 'Met SLA' THEN 1 ELSE 0 END) / COUNT(*), 2) as sla_compliance_pct
# MAGIC FROM pipeline_runs
# MAGIC WHERE sla_status IN ('Met SLA', 'SLA Violation')
# MAGIC GROUP BY pipeline_name, run_date
# MAGIC ORDER BY run_date DESC, pipeline_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Freshness SLA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor data freshness for gold layer tables
# MAGIC WITH table_freshness AS (
# MAGIC   SELECT 
# MAGIC     table_catalog || '.' || table_schema || '.' || table_name as full_table_name,
# MAGIC     MAX(created_timestamp) as last_updated,
# MAGIC     TIMESTAMPDIFF(HOUR, MAX(created_timestamp), CURRENT_TIMESTAMP()) as hours_since_update,
# MAGIC     CASE 
# MAGIC       WHEN TIMESTAMPDIFF(HOUR, MAX(created_timestamp), CURRENT_TIMESTAMP()) <= 2 THEN 'Met SLA'
# MAGIC       WHEN TIMESTAMPDIFF(HOUR, MAX(created_timestamp), CURRENT_TIMESTAMP()) <= 4 THEN 'Warning'
# MAGIC       ELSE 'SLA Violation'
# MAGIC     END as freshness_status
# MAGIC   FROM financial_lakehouse.reporting.account_performance
# MAGIC   GROUP BY table_catalog, table_schema, table_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   full_table_name,
# MAGIC   last_updated,
# MAGIC   hours_since_update,
# MAGIC   freshness_status
# MAGIC FROM table_freshness
# MAGIC ORDER BY hours_since_update DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Performance SLA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track query execution times and P95 latency
# MAGIC WITH query_metrics AS (
# MAGIC   SELECT 
# MAGIC     statement_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
# MAGIC     statement_text,
# MAGIC     user_identity.email as user_email,
# MAGIC     CASE 
# MAGIC       WHEN TIMESTAMPDIFF(SECOND, start_time, end_time) <= 5 THEN 'Met SLA'
# MAGIC       WHEN TIMESTAMPDIFF(SECOND, start_time, end_time) <= 10 THEN 'Warning'
# MAGIC       ELSE 'SLA Violation'
# MAGIC     END as performance_status
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC     AND statement_text LIKE '%financial_lakehouse%'
# MAGIC )
# MAGIC SELECT 
# MAGIC   DATE(start_time) as query_date,
# MAGIC   COUNT(*) as total_queries,
# MAGIC   ROUND(AVG(duration_seconds), 2) as avg_duration_sec,
# MAGIC   ROUND(PERCENTILE(duration_seconds, 0.50), 2) as p50_duration_sec,
# MAGIC   ROUND(PERCENTILE(duration_seconds, 0.95), 2) as p95_duration_sec,
# MAGIC   ROUND(PERCENTILE(duration_seconds, 0.99), 2) as p99_duration_sec,
# MAGIC   SUM(CASE WHEN performance_status = 'Met SLA' THEN 1 ELSE 0 END) as queries_met_sla,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN performance_status = 'Met SLA' THEN 1 ELSE 0 END) / COUNT(*), 2) as sla_compliance_pct
# MAGIC FROM query_metrics
# MAGIC GROUP BY query_date
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Availability SLA (Table Uptime)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate table availability (99.9% uptime target)
# MAGIC WITH hourly_checks AS (
# MAGIC   SELECT 
# MAGIC     DATE_TRUNC('HOUR', event_time) as check_hour,
# MAGIC     request_params.full_name_arg as table_name,
# MAGIC     COUNT(*) as access_count,
# MAGIC     SUM(CASE WHEN response.status_code < 400 THEN 1 ELSE 0 END) as successful_access,
# MAGIC     SUM(CASE WHEN response.status_code >= 400 THEN 1 ELSE 0 END) as failed_access
# MAGIC   FROM system.access.audit
# MAGIC   WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC     AND action_name = 'getTableData'
# MAGIC     AND request_params.full_name_arg LIKE 'financial_lakehouse.reporting.%'
# MAGIC   GROUP BY check_hour, table_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   COUNT(DISTINCT check_hour) as total_hours_checked,
# MAGIC   SUM(successful_access) as total_successful,
# MAGIC   SUM(failed_access) as total_failed,
# MAGIC   ROUND(100.0 * SUM(successful_access) / (SUM(successful_access) + SUM(failed_access)), 3) as availability_pct,
# MAGIC   CASE 
# MAGIC     WHEN 100.0 * SUM(successful_access) / (SUM(successful_access) + SUM(failed_access)) >= 99.9 THEN 'Met SLA'
# MAGIC     ELSE 'SLA Violation'
# MAGIC   END as sla_status
# MAGIC FROM hourly_checks
# MAGIC GROUP BY table_name
# MAGIC ORDER BY availability_pct;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SLA Dashboard Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combined SLA metrics dashboard
# MAGIC SELECT 
# MAGIC   'Pipeline Execution' as sla_category,
# MAGIC   '< 60 min' as sla_target,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, start_time, end_time) <= 60 THEN 1 ELSE 0 END) / COUNT(*), 2) as current_compliance_pct,
# MAGIC   99.0 as target_compliance_pct
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   AND event_type = 'update'
# MAGIC   AND status = 'COMPLETED'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Query Performance' as sla_category,
# MAGIC   'P95 < 5 sec' as sla_target,
# MAGIC   CASE 
# MAGIC     WHEN PERCENTILE(TIMESTAMPDIFF(SECOND, start_time, end_time), 0.95) <= 5 THEN 100.0
# MAGIC     ELSE ROUND(100.0 * (1 - (PERCENTILE(TIMESTAMPDIFF(SECOND, start_time, end_time), 0.95) - 5.0) / 10.0), 2)
# MAGIC   END as current_compliance_pct,
# MAGIC   95.0 as target_compliance_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC   AND statement_text LIKE '%financial_lakehouse%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SLA Violation Alerts

# COMMAND ----------

# Detect SLA violations and generate alerts
def check_sla_violations():
    """
    Check for SLA violations and return alert summary
    """
    violations = []
    
    # Check pipeline execution SLA
    pipeline_sla = spark.sql("""
        SELECT 
            pipeline_name,
            COUNT(*) as total_runs,
            SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, start_time, end_time) > 60 THEN 1 ELSE 0 END) as violations
        FROM system.lakeflow.pipeline_events
        WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAYS
          AND event_type = 'update'
          AND status = 'COMPLETED'
        GROUP BY pipeline_name
        HAVING violations > 0
    """).collect()
    
    for row in pipeline_sla:
        violations.append({
            'sla_type': 'Pipeline Execution',
            'resource': row['pipeline_name'],
            'violation_count': row['violations'],
            'total_count': row['total_runs']
        })
    
    # Display violations
    if violations:
        print("🚨 SLA VIOLATIONS DETECTED:\n")
        for v in violations:
            print(f"   • {v['sla_type']}: {v['resource']}")
            print(f"     Violations: {v['violation_count']} / {v['total_count']}")
    else:
        print("✅ No SLA violations in the last 24 hours")
    
    return violations

violations = check_sla_violations()

# COMMAND ----------

print("✅ SLA tracking queries complete")
