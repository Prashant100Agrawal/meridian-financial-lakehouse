# Databricks notebook source
# DBTITLE 1,Header
# Databricks notebook source
# MAGIC %md
# MAGIC # Check and Send Alerts
# MAGIC ## Runs after orchestration job to check alert views and send notifications

# COMMAND ----------

# DBTITLE 1,Alert Check and Send
# Databricks notebook source
# MAGIC %md
# MAGIC # Check and Send Alerts
# MAGIC ## Runs after orchestration job to check alert views and send notifications

# COMMAND ----------

import json

# COMMAND ----------

def send_alert(alert_type, alert_message, channels=['email', 'slack']):
    """Send alert to configured channels using secrets"""
    print(f"ALERT [{alert_type}]: {alert_message}")
    
    if 'slack' in channels:
        try:
            import requests
            webhook_url = dbutils.secrets.get(scope='meridian_alerts', key='slack_webhook_url')
            resp = requests.post(webhook_url, json={'text': f'[{alert_type}] {alert_message}'}, timeout=10)
            print(f'  Slack sent: {resp.status_code}')
        except Exception as e:
            print(f'  Slack failed: {e}')
    
    if 'email' in channels:
        try:
            alert_email = dbutils.secrets.get(scope='meridian_alerts', key='alert_email')
            print(f'  Email to: {alert_email}')
        except Exception as e:
            print(f'  Email failed: {e}')

# COMMAND ----------

alerts_sent = 0

# Check pipeline failures
print('=== Checking Pipeline Failure Alerts ===')
try:
    failures = spark.sql('SELECT * FROM financial_lakehouse.reporting.pipeline_failure_alerts').collect()
    for f in failures:
        send_alert('PIPELINE_FAILURE', f['alert_message'], ['email', 'slack'])
        alerts_sent += 1
except Exception as e:
    print(f'No failure alerts: {e}')

# Check SLA violations
print('\n=== Checking SLA Violations ===')
try:
    violations = spark.sql('SELECT * FROM financial_lakehouse.reporting.pipeline_sla_violations').collect()
    for v in violations:
        send_alert('SLA_VIOLATION', v['alert_message'], ['email'])
        alerts_sent += 1
except Exception as e:
    print(f'No SLA violations: {e}')

# Check freshness violations
print('\n=== Checking Freshness Violations ===')
try:
    freshness = spark.sql('SELECT * FROM financial_lakehouse.reporting.freshness_sla_violations').collect()
    for f in freshness:
        send_alert('FRESHNESS_VIOLATION', f['alert_message'], ['email', 'slack'])
        alerts_sent += 1
except Exception as e:
    print(f'No freshness violations: {e}')

# Check DQ failures
print('\n=== Checking DQ Failures ===')
try:
    dq = spark.sql('SELECT * FROM financial_lakehouse.reporting.dq_failure_alerts').collect()
    for d in dq:
        send_alert('DQ_FAILURE', d['alert_message'], ['email', 'slack'])
        alerts_sent += 1
except Exception as e:
    print(f'No DQ failures: {e}')

# Check cost thresholds
print('\n=== Checking Cost Thresholds ===')
try:
    costs = spark.sql('SELECT * FROM financial_lakehouse.reporting.cost_threshold_alerts').collect()
    for c in costs:
        send_alert('COST_THRESHOLD', c['alert_message'], ['email'])
        alerts_sent += 1
except Exception as e:
    print(f'No cost alerts: {e}')

print(f'\n=== Summary: {alerts_sent} alerts sent ===')

# Log to pipeline_logs table
from datetime import datetime
from pyspark.sql import Row

log_row = Row(
    log_timestamp=datetime.now(),
    pipeline_name='alert_checker',
    log_level='INFO',
    message=f'Alert check complete: {alerts_sent} alerts sent',
    step_name=None,
    records_processed=alerts_sent,
    duration_seconds=None,
    error_details=None,
    run_id=datetime.now().strftime('%Y%m%d_%H%M%S'),
    created_by=spark.sql('SELECT current_user() as u').collect()[0][0]
)
spark.createDataFrame([log_row]).write.mode('append').saveAsTable('financial_lakehouse.operational.pipeline_logs')
print('Logged to pipeline_logs table')