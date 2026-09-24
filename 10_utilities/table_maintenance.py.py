# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Table Maintenance: VACUUM & OPTIMIZE
# Databricks notebook source
# MAGIC %md
# MAGIC # Table Maintenance: VACUUM & OPTIMIZE
# MAGIC ## Runs Delta Lake maintenance on all lakehouse tables
# MAGIC ### Schedule: Weekly (add as task in orchestration job)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Define all managed tables for maintenance
tables = [
    'financial_lakehouse.operational.trading_systems',
    'financial_lakehouse.operational.pipeline_logs',
    'financial_lakehouse.operational.dlt_event_logs',
    'financial_lakehouse.standardized.trading_systems_clean',
    'financial_lakehouse.standardized.trading_systems_conformed',
    'financial_lakehouse.standardized.trading_systems_scd2',
    'financial_lakehouse.reporting.account_performance',
    'financial_lakehouse.reporting.daily_trading_summary',
    'financial_lakehouse.reporting.nav_calculations',
    'financial_lakehouse.reporting.profit_loss',
    'financial_lakehouse.reporting.audit_log_summary',
    'financial_lakehouse.reporting.data_quality_metrics',
    'financial_lakehouse.reporting.data_retention_policies',
    'financial_lakehouse.reporting.dim_accounts',
    'financial_lakehouse.reporting.dim_currencies',
    'financial_lakehouse.reporting.dim_instruments',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OPTIMIZE Tables (Compact small files)

# COMMAND ----------

print('=== OPTIMIZE: Compacting Delta files ===')
optimized = 0
for table in tables:
    try:
        spark.sql(f'OPTIMIZE {table}')
        print(f'  Optimized: {table}')
        optimized += 1
    except Exception as e:
        print(f'  Skipped: {table} ({str(e)[:80]})')
print(f'\nTotal optimized: {optimized}/{len(tables)}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VACUUM Tables (Remove old files past retention)

# COMMAND ----------

print('\n=== VACUUM: Removing old Delta files ===')
vacuumed = 0
for table in tables:
    try:
        # VACUUM with 7-day retention (10080 minutes)
        spark.sql(f'VACUUM {table} RETAIN 10080 HOURS')
        print(f'  Vacuumed: {table}')
        vacuumed += 1
    except Exception as e:
        print(f'  Skipped: {table} ({str(e)[:80]})')
print(f'\nTotal vacuumed: {vacuumed}/{len(tables)}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Summary Report

# COMMAND ----------

print(f'\n=== Maintenance Summary ===')
print(f'  OPTIMIZE: {optimized}/{len(tables)} tables')
print(f'  VACUUM:   {vacuumed}/{len(tables)} tables')
print(f'  Timestamp: {current_timestamp()}')

# Log maintenance run
from datetime import datetime
from pyspark.sql import Row
log_row = Row(
    log_timestamp=datetime.now(),
    pipeline_name='table_maintenance',
    log_level='INFO',
    message=f'Maintenance complete: {optimized} optimized, {vacuumed} vacuumed',
    step_name='vacuum_optimize',
    records_processed=optimized + vacuumed,
    duration_seconds=None,
    error_details=None,
    run_id=datetime.now().strftime('%Y%m%d_%H%M%S'),
    created_by=spark.sql('SELECT current_user() as u').collect()[0][0]
)
spark.createDataFrame([log_row]).write.mode('append').saveAsTable('financial_lakehouse.operational.pipeline_logs')
print('Logged to pipeline_logs table')