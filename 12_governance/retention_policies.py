# Databricks notebook source
# MAGIC %md
# MAGIC # Data Retention Policies
# MAGIC ## Automated data lifecycle management and policy-based retention
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Retention Requirements
# MAGIC * Trading data: 7 years (regulatory requirement)
# MAGIC * Risk data: 7 years
# MAGIC * Compliance reports: 10 years
# MAGIC * Operational logs: 90 days
# MAGIC * Temporary/staging data: 7 days

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Retention Policies

# COMMAND ----------

# Retention policy configuration
retention_policies = {
    'trading_data': {
        'retention_days': 2555,  # 7 years
        'tables': [
            'financial_lakehouse.operational.trading_systems',
            'financial_lakehouse.standardized.trading_systems_clean',
            'financial_lakehouse.reporting.daily_trading_summary'
        ]
    },
    'risk_compliance': {
        'retention_days': 2555,  # 7 years
        'tables': [
            'financial_lakehouse.reporting.risk_measures',
            'financial_lakehouse.reporting.regulatory_reporting'
        ]
    },
    'analytics': {
        'retention_days': 1825,  # 5 years
        'tables': [
            'financial_lakehouse.reporting.account_performance',
            'financial_lakehouse.reporting.portfolio_performance'
        ]
    },
    'operational_logs': {
        'retention_days': 90,  # 90 days
        'tables': [
            'financial_lakehouse.operational.api_ingestion_logs',
            'financial_lakehouse.operational.pipeline_execution_logs'
        ]
    },
    'staging_temp': {
        'retention_days': 7,  # 7 days
        'tables': [
            'financial_lakehouse.operational.staging_temp'
        ]
    }
}

# Display policies
for policy_name, config in retention_policies.items():
    print(f"📋 {policy_name}: {config['retention_days']} days ({config['retention_days']//365} years)")
    for table in config['tables']:
        print(f"   • {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Retention Metadata Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS financial_lakehouse.reporting.data_retention_policies (
# MAGIC   policy_name STRING,
# MAGIC   table_name STRING,
# MAGIC   retention_days INT,
# MAGIC   date_column STRING,
# MAGIC   last_cleanup_date TIMESTAMP,
# MAGIC   records_deleted BIGINT,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Data retention policies and cleanup history';

# COMMAND ----------

# Insert retention policies
from pyspark.sql.types import *

# Create policy records
policy_records = []
for policy_name, config in retention_policies.items():
    for table in config['tables']:
        policy_records.append({
            'policy_name': policy_name,
            'table_name': table,
            'retention_days': config['retention_days'],
            'date_column': 'trade_date' if 'trading' in table else 'created_timestamp',
            'last_cleanup_date': None,
            'records_deleted': 0,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })

# Write to table
policy_df = spark.createDataFrame(policy_records)
policy_df.write.mode("overwrite").saveAsTable("financial_lakehouse.reporting.data_retention_policies")

print(f"✅ Created {len(policy_records)} retention policies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retention Policy Enforcement Function

# COMMAND ----------

def enforce_retention_policy(table_name, retention_days, date_column='created_timestamp', dry_run=True):
    """
    Delete records older than retention period
    
    Args:
        table_name: Full table name (catalog.schema.table)
        retention_days: Number of days to retain
        date_column: Column to use for age calculation
        dry_run: If True, only count records without deleting
    """
    cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
    
    # Count records to delete
    count_query = f"""
        SELECT COUNT(*) as count
        FROM {table_name}
        WHERE {date_column} < '{cutoff_date}'
    """
    
    try:
        records_to_delete = spark.sql(count_query).collect()[0]['count']
        
        if records_to_delete == 0:
            print(f"✅ {table_name}: No records to delete (cutoff: {cutoff_date})")
            return 0
        
        if dry_run:
            print(f"🔍 DRY RUN - {table_name}: Would delete {records_to_delete:,} records older than {cutoff_date}")
            return records_to_delete
        
        # Execute deletion
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE {date_column} < '{cutoff_date}'
        """
        spark.sql(delete_query)
        
        # Update retention policy table
        update_query = f"""
            UPDATE financial_lakehouse.reporting.data_retention_policies
            SET last_cleanup_date = CURRENT_TIMESTAMP(),
                records_deleted = records_deleted + {records_to_delete},
                updated_at = CURRENT_TIMESTAMP()
            WHERE table_name = '{table_name}'
        """
        spark.sql(update_query)
        
        print(f"✅ {table_name}: Deleted {records_to_delete:,} records older than {cutoff_date}")
        return records_to_delete
        
    except Exception as e:
        print(f"❌ Error processing {table_name}: {str(e)}")
        return 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Retention Policy (Dry Run)

# COMMAND ----------

# Run retention cleanup in dry-run mode
total_to_delete = 0

for policy_name, config in retention_policies.items():
    print(f"\n{'='*60}")
    print(f"📋 Policy: {policy_name} (Retention: {config['retention_days']} days)")
    print('='*60)
    
    for table in config['tables']:
        # Determine date column based on table
        if 'trading' in table or 'daily' in table:
            date_col = 'trade_date'
        elif 'bronze' in table:
            date_col = '_bronze_ingestion_timestamp'
        else:
            date_col = 'created_timestamp'
        
        # Check if table exists before processing
        try:
            spark.sql(f"DESCRIBE TABLE {table}")
            count = enforce_retention_policy(table, config['retention_days'], date_col, dry_run=True)
            total_to_delete += count
        except Exception as e:
            print(f"⚠️  Table {table} not found or inaccessible: {str(e)}")

print(f"\n{'='*60}")
print(f"📊 Total records to delete across all policies: {total_to_delete:,}")
print('='*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Retention Policy (PRODUCTION)

# COMMAND ----------

# PRODUCTION: Uncomment to actually delete data
# WARNING: This will permanently delete data!

# total_deleted = 0
# 
# for policy_name, config in retention_policies.items():
#     print(f"\n{'='*60}")
#     print(f"📋 Policy: {policy_name} (Retention: {config['retention_days']} days)")
#     print('='*60)
#     
#     for table in config['tables']:
#         if 'trading' in table or 'daily' in table:
#             date_col = 'trade_date'
#         elif 'bronze' in table:
#             date_col = '_bronze_ingestion_timestamp'
#         else:
#             date_col = 'created_timestamp'
#         
#         try:
#             count = enforce_retention_policy(table, config['retention_days'], date_col, dry_run=False)
#             total_deleted += count
#         except Exception as e:
#             print(f"❌ Error: {str(e)}")
# 
# print(f"\n{'='*60}")
# print(f"✅ Total records deleted: {total_deleted:,}")
# print('='*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Retention Compliance Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View retention policy status
# MAGIC SELECT 
# MAGIC #     policy_name,
# MAGIC #     table_name,
# MAGIC #     retention_days,
# MAGIC #     ROUND(retention_days / 365.0, 1) as retention_years,
# MAGIC #     last_cleanup_date,
# MAGIC #     records_deleted,
# MAGIC #     CASE 
# MAGIC #         WHEN last_cleanup_date IS NULL THEN 'Never Run'
# MAGIC #         WHEN DATEDIFF(CURRENT_DATE(), DATE(last_cleanup_date)) > 30 THEN 'Overdue'
# MAGIC #         ELSE 'Current'
# MAGIC #     END as cleanup_status
# MAGIC # FROM financial_lakehouse.reporting.data_retention_policies
# MAGIC # ORDER BY policy_name, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Check Table Sizes and Oldest Records

# COMMAND ----------

def check_table_age_and_size(table_name, date_column):
    """
    Check table size and oldest/newest records
    """
    try:
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN({date_column}) as oldest_record,
                MAX({date_column}) as newest_record,
                DATEDIFF(CURRENT_DATE(), MIN({date_column})) as age_days
            FROM {table_name}
        """
        result = spark.sql(query).collect()[0]
        
        print(f"📊 {table_name}")
        print(f"   Total Records: {result['total_records']:,}")
        print(f"   Oldest Record: {result['oldest_record']}")
        print(f"   Newest Record: {result['newest_record']}")
        print(f"   Data Age: {result['age_days']} days ({result['age_days']//365} years)")
        print()
        
    except Exception as e:
        print(f"❌ {table_name}: {str(e)}\n")

# Check all tables
for policy_name, config in retention_policies.items():
    for table in config['tables']:
        if 'trading' in table or 'daily' in table:
            date_col = 'trade_date'
        elif 'bronze' in table:
            date_col = '_bronze_ingestion_timestamp'
        else:
            date_col = 'created_timestamp'
        check_table_age_and_size(table, date_col)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Schedule Retention Job

# COMMAND ----------

# Instructions for scheduling this notebook as a job
print("""
📅 Scheduling Recommendations:

1. Create a Databricks Job to run this notebook weekly/monthly
2. Use the workflow_runner notebook to orchestrate
3. Set up alerts for:
   - Failed retention cleanup
   - Tables exceeding retention limits
   - Large deletion operations (> 1M records)

Sample Job Config (JSON):
{
  "name": "Data Retention Cleanup",
  "schedule": {
    "quartz_cron_expression": "0 0 2 ? * SUN",  // 2 AM every Sunday
    "timezone_id": "America/New_York"
  },
  "tasks": [{
    "task_key": "retention_cleanup",
    "notebook_task": {
      "notebook_path": "/Repos/.../11_governance/retention_policies",
      "base_parameters": {
        "dry_run": "false"
      }
    }
  }]
}
""")

# COMMAND ----------

print("✅ Retention policies configured and validated")