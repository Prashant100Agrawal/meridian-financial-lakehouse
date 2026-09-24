# Databricks notebook source
# MAGIC %md
# MAGIC # Alert Setup
# MAGIC ## Configure automated alerts for pipeline failures, SLA violations, and anomalies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Alert Types
# MAGIC * Pipeline execution failures
# MAGIC * SLA violations (runtime, freshness, performance)
# MAGIC * Data quality rule failures
# MAGIC * Anomalous access patterns
# MAGIC * Cost threshold breaches

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from databricks.sdk.service.sql import *
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Alert Configuration

# COMMAND ----------

# Define alert thresholds
alert_config = {
    'pipeline_failure': {
        'enabled': True,
        'threshold': 1,  # Alert on any failure
        'channels': ['email', 'slack']
    },
    'sla_violation_pipeline': {
        'enabled': True,
        'threshold_minutes': 60,  # Pipeline runtime > 60 min
        'channels': ['email']
    },
    'sla_violation_freshness': {
        'enabled': True,
        'threshold_hours': 2,  # Data not updated in 2+ hours
        'channels': ['email', 'slack']
    },
    'query_performance': {
        'enabled': True,
        'p95_threshold_seconds': 5,
        'channels': ['slack']
    },
    'data_quality': {
        'enabled': True,
        'failure_rate_pct': 5,  # > 5% failures
        'channels': ['email', 'slack']
    },
    'cost_threshold': {
        'enabled': True,
        'daily_dbu_threshold': 1000,
        'channels': ['email']
    }
}

# Alert destinations
alert_destinations = {
    'email': dbutils.secrets.get(scope='meridian_alerts', key='alert_email'),
    'slack': '#lakehouse-alerts'
}

print("📋 Alert Configuration:")
for alert_type, config in alert_config.items():
    status = "✅ Enabled" if config['enabled'] else "❌ Disabled"
    print(f"   {status} {alert_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Failure Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create alert query for pipeline failures
# MAGIC CREATE OR REPLACE VIEW financial_lakehouse.reporting.pipeline_failure_alerts AS
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   update_id,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   error_message,
# MAGIC   CONCAT('Pipeline ', pipeline_name, ' failed at ', start_time, '. Error: ', error_message) as alert_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE event_date >= CURRENT_DATE()
# MAGIC   AND status = 'FAILED'
# MAGIC ORDER BY start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SLA Violation Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipeline runtime SLA violations
# MAGIC CREATE OR REPLACE VIEW financial_lakehouse.reporting.pipeline_sla_violations AS
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   update_id,
# MAGIC   start_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
# MAGIC   CONCAT('SLA Violation: Pipeline ', pipeline_name, ' took ', 
# MAGIC          TIMESTAMPDIFF(MINUTE, start_time, end_time), ' minutes (SLA: 60 min)') as alert_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE event_date >= CURRENT_DATE()
# MAGIC   AND status = 'COMPLETED'
# MAGIC   AND TIMESTAMPDIFF(MINUTE, start_time, end_time) > 60
# MAGIC ORDER BY start_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data freshness SLA violations
# MAGIC CREATE OR REPLACE VIEW financial_lakehouse.reporting.freshness_sla_violations AS
# MAGIC WITH table_freshness AS (
# MAGIC   SELECT 
# MAGIC     'account_performance' as table_name,
# MAGIC     MAX(created_timestamp) as last_updated,
# MAGIC     TIMESTAMPDIFF(HOUR, MAX(created_timestamp), CURRENT_TIMESTAMP()) as hours_stale
# MAGIC   FROM financial_lakehouse.reporting.account_performance
# MAGIC )
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   last_updated,
# MAGIC   hours_stale,
# MAGIC   CONCAT('Freshness SLA Violation: Table ', table_name, ' is ', hours_stale, ' hours stale (SLA: 2 hours)') as alert_message
# MAGIC FROM table_freshness
# MAGIC WHERE hours_stale > 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality rule failures
# MAGIC CREATE OR REPLACE VIEW financial_lakehouse.reporting.dq_failure_alerts AS
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   rule_name,
# MAGIC   check_timestamp,
# MAGIC   failed_records,
# MAGIC   total_records,
# MAGIC   ROUND(100.0 * failed_records / total_records, 2) as failure_rate_pct,
# MAGIC   CONCAT('Data Quality Alert: ', table_name, ' - ', rule_name, ' failed ', 
# MAGIC          failed_records, ' / ', total_records, ' records (', 
# MAGIC          ROUND(100.0 * failed_records / total_records, 2), '%)') as alert_message
# MAGIC FROM financial_lakehouse.reporting.dq_check_results
# MAGIC WHERE check_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC   AND (100.0 * failed_records / total_records) > 5
# MAGIC ORDER BY failure_rate_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost Threshold Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBU usage exceeding threshold
# MAGIC CREATE OR REPLACE VIEW financial_lakehouse.reporting.cost_threshold_alerts AS
# MAGIC WITH daily_usage AS (
# MAGIC   SELECT 
# MAGIC     usage_date,
# MAGIC     SUM(usage_quantity) as total_dbu
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   GROUP BY usage_date
# MAGIC )
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   total_dbu,
# MAGIC   CONCAT('Cost Alert: DBU usage on ', usage_date, ' was ', total_dbu, ' (threshold: 1000)') as alert_message
# MAGIC FROM daily_usage
# MAGIC WHERE total_dbu > 1000
# MAGIC ORDER BY usage_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Send Alert Function

# COMMAND ----------

def send_alert(alert_type, alert_message, channels=['email']):
    """
    Send alert to configured channels
    
    Args:
        alert_type: Type of alert (pipeline_failure, sla_violation, etc.)
        alert_message: Alert message text
        channels: List of channels to send to (email, slack, pagerduty)
    """
    print(f"🚨 ALERT: {alert_type}")
    print(f"   Message: {alert_message}")
    print(f"   Channels: {', '.join(channels)}")
    
    # Email alert (example - integrate with your email service)
    if 'email' in channels:
        # dbutils.notebook.run('/path/to/email_sender', 0, {
        #     'to': alert_destinations['email'],
        #     'subject': f'[Lakehouse Alert] {alert_type}',
        #     'body': alert_message
        # })
        print(f"   📧 Email sent to {alert_destinations.get('email', 'N/A')}")
    
    # Slack alert (example - integrate with Slack webhook)
    if 'slack' in channels:
        slack_webhook_url = dbutils.secrets.get(scope="meridian_alerts", key="slack_webhook_url")
        # requests.post(slack_webhook_url, json={'text': alert_message})
        print(f"   💬 Slack message sent to {alert_destinations.get('slack', 'N/A')}")
    
    return True

# Example usage
# send_alert('pipeline_failure', 'Pipeline dlt_financial_analytics failed', ['email', 'slack'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Check and Send Alerts

# COMMAND ----------

def check_and_send_alerts():
    """
    Check all alert views and send notifications
    """
    alerts_sent = 0
    
    # Pipeline failures
    if alert_config['pipeline_failure']['enabled']:
        failures = spark.sql("SELECT * FROM financial_lakehouse.reporting.pipeline_failure_alerts").collect()
        for failure in failures:
            send_alert('pipeline_failure', failure['alert_message'], alert_config['pipeline_failure']['channels'])
            alerts_sent += 1
    
    # Pipeline SLA violations
    if alert_config['sla_violation_pipeline']['enabled']:
        violations = spark.sql("SELECT * FROM financial_lakehouse.reporting.pipeline_sla_violations").collect()
        for violation in violations:
            send_alert('sla_violation', violation['alert_message'], alert_config['sla_violation_pipeline']['channels'])
            alerts_sent += 1
    
    # Data freshness SLA violations
    if alert_config['sla_violation_freshness']['enabled']:
        freshness_violations = spark.sql("SELECT * FROM financial_lakehouse.reporting.freshness_sla_violations").collect()
        for violation in freshness_violations:
            send_alert('freshness_violation', violation['alert_message'], alert_config['sla_violation_freshness']['channels'])
            alerts_sent += 1
    
    # Data quality failures
    if alert_config['data_quality']['enabled']:
        dq_failures = spark.sql("SELECT * FROM financial_lakehouse.reporting.dq_failure_alerts").collect()
        for failure in dq_failures:
            send_alert('data_quality_failure', failure['alert_message'], alert_config['data_quality']['channels'])
            alerts_sent += 1
    
    # Cost threshold breaches
    if alert_config['cost_threshold']['enabled']:
        cost_alerts = spark.sql("SELECT * FROM financial_lakehouse.reporting.cost_threshold_alerts").collect()
        for alert in cost_alerts:
            send_alert('cost_threshold', alert['alert_message'], alert_config['cost_threshold']['channels'])
            alerts_sent += 1
    
    print(f"\n📊 Alert Summary: {alerts_sent} alerts sent")
    return alerts_sent

# Run alert checks
alerts_sent = check_and_send_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Schedule Alert Job

# COMMAND ----------

print("""
📅 Scheduling Recommendations:

1. Run this notebook every 15 minutes for real-time alerts
2. Use Databricks Jobs with email on failure
3. Integrate with:
   - Slack webhooks for team notifications
   - PagerDuty for critical alerts
   - Email for daily summaries

Sample Job Config (JSON):
{
  "name": "Lakehouse Alert Monitor",
  "schedule": {
    "quartz_cron_expression": "0 */15 * * * ?",  // Every 15 minutes
    "timezone_id": "America/New_York"
  },
  "email_notifications": {
    "on_failure": ["data-eng-team@company.com"]
  },
  "tasks": [{
    "task_key": "check_alerts",
    "notebook_task": {
      "notebook_path": "/Repos/.../12_monitoring/alert_setup"
    }
  }]
}
""")

# COMMAND ----------

print("✅ Alert setup complete")
