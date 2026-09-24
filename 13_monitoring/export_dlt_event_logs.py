# Databricks notebook source
# MAGIC %md
# MAGIC # Export DLT Pipeline Event Logs
# MAGIC ## Exports SDP/DLT pipeline events to a queryable Delta table
# MAGIC
# MAGIC **Target table:** `financial_lakehouse.operational.dlt_event_logs`
# MAGIC
# MAGIC **Schedule:** Run after each pipeline update or hourly via Lakeflow Jobs

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

# COMMAND ----------

# Pipeline configuration
PIPELINE_ID = "c087d1c3-bcc3-45bf-9597-d4d361d48493"  # Meridian Financial Analytics SDP
TARGET_TABLE = "financial_lakehouse.operational.dlt_event_logs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Query DLT Event Log

# COMMAND ----------

try:
    # Query the DLT event log using the event_log table-valued function
    # This captures all pipeline events: updates, flow progress, expectations, errors
    events_df = spark.sql(f"""
        SELECT 
            timestamp as event_time,
            origin.update_id as update_id,
            message,
            event_type,
            level,
            error,
            origin.pipeline_id as pipeline_id,
            version
        FROM event_log('{PIPELINE_ID}')
        WHERE timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
    """)
    
    event_count = events_df.count()
    print(f"✅ Retrieved {event_count} DLT events from pipeline {PIPELINE_ID}")
    
except Exception as e:
    print(f"⚠️ Could not query event log: {e}")
    print("This is expected if the pipeline hasn't run yet or event logs have expired.")
    print("Creating empty dataframe for schema reference...")
    events_df = spark.createDataFrame([], schema="""
        event_time TIMESTAMP, update_id STRING, message STRING,
        event_type STRING, level STRING, error STRING,
        pipeline_id STRING, version STRING
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Events to Delta Table

# COMMAND ----------

if events_df.count() > 0:
    # Add capture timestamp
    from pyspark.sql.functions import current_timestamp
    events_with_meta = events_df.withColumn("captured_at", current_timestamp())
    
    # Select and rename columns to match target schema
    events_final = events_with_meta.select(
        "captured_at",
        "pipeline_id",
        "update_id",
        "event_type",
        "event_time",
        col("origin").alias("origin"),
        "message",
        col("error").alias("error_details"),
        "level",
        "version"
    )
    
    # Write to Delta table (append mode for historical accumulation)
    events_final.write.mode("append").saveAsTable(TARGET_TABLE)
    print(f"✅ Wrote {events_final.count()} events to {TARGET_TABLE}")
else:
    print("ℹ️ No events to write. Pipeline may not have run recently.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Show Recent Events Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        event_type,
        level,
        COUNT(*) as event_count,
        MIN(event_time) as earliest_event,
        MAX(event_time) as latest_event
    FROM {TARGET_TABLE}
    GROUP BY event_type, level
    ORDER BY event_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alert-Ready Queries
# MAGIC ### These queries can be used for alerting on pipeline health

# COMMAND ----------

# Failed events in last 24 hours
display(spark.sql(f"""
    SELECT event_time, event_type, message, error_details
    FROM {TARGET_TABLE}
    WHERE level IN ('ERROR', 'WARN')
      AND event_time >= CURRENT_DATE() - INTERVAL 1 DAYS
    ORDER BY event_time DESC
    LIMIT 20
"""))

# COMMAND ----------

print("✅ DLT event log export complete")
