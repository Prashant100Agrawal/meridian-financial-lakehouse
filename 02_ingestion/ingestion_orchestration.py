# Databricks notebook source
# DBTITLE 1,Ingestion Orchestration & Monitoring
# MAGIC %md
# MAGIC # Ingestion Orchestration & Monitoring Dashboard
# MAGIC ## Central control and monitoring for all ingestion channels
# MAGIC
# MAGIC This notebook provides:
# MAGIC * **Channel Status** - Monitor all ingestion channels
# MAGIC * **Data Quality** - Track record counts, freshness, and completeness
# MAGIC * **Error Handling** - Identify and alert on failures
# MAGIC * **Orchestration** - Run ingestion channels on schedule

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Ingestion Channel Inventory
# Define all ingestion channels
ingestion_channels = {
    "api_ingestion": {
        "name": "API Ingestion",
        "type": "Batch",
        "source": "External APIs",
        "frequency": "Every 15 minutes",
        "target": "financial_lakehouse.raw",
        "notebook": "api_ingest_raw",
        "status": "Active"
    },
    "streaming_autoloader": {
        "name": "Auto Loader Streaming",
        "type": "Streaming",
        "source": "Landing Files",
        "frequency": "Continuous",
        "target": "financial_lakehouse.operational",
        "notebook": "streaming_ingest",
        "status": "Active"
    },
    "kafka_msk": {
        "name": "Kafka/MSK Streaming",
        "type": "Streaming",
        "source": "Kafka Topics",
        "frequency": "Real-time",
        "target": "financial_lakehouse.operational.trading_events_stream",
        "notebook": "kafka_msk_streaming",
        "status": "Configured"
    },
    "dms_cdc": {
        "name": "DMS CDC",
        "type": "Streaming",
        "source": "Database Changes",
        "frequency": "Real-time",
        "target": "financial_lakehouse.operational",
        "notebook": "dms_cdc_ingest",
        "status": "Configured"
    },
    "autoloader_enhanced": {
        "name": "Enhanced Auto Loader",
        "type": "Streaming",
        "source": "Multiple Formats",
        "frequency": "Continuous",
        "target": "financial_lakehouse.operational",
        "notebook": "autoloader_enhanced",
        "status": "Configured"
    },
    "s3_lambda_controlled": {
        "name": "S3 + Lambda Controlled Uploads",
        "type": "Event-driven",
        "source": "S3/Volume Events",
        "frequency": "On file arrival",
        "target": "financial_lakehouse.operational",
        "notebook": "s3_lambda_controlled_uploads",
        "status": "Configured"
    }
}

print(f"✓ Defined {len(ingestion_channels)} ingestion channels")

# COMMAND ----------

# DBTITLE 1,Data Freshness Check
# Check data freshness across all sources
print("\n" + "="*80)
print("DATA FRESHNESS REPORT")
print("="*80)

data_sources = [
    ("Trading Systems", "financial_lakehouse.operational.trading_systems"),
    ("Raw Trading Stream", "financial_lakehouse.raw.trading_systems_stream"),
    ("Silver Conformed", "financial_lakehouse.standardized.trading_systems_conformed")
]

for source_name, table_name in data_sources:
    try:
        # Check if table exists
        if spark.catalog.tableExists(table_name):
            # Get record count and freshness
            result = spark.sql(f"""
                SELECT 
                    COUNT(*) as record_count,
                    MAX(ingestion_timestamp) as latest_ingestion,
                    MIN(ingestion_timestamp) as earliest_ingestion,
                    DATEDIFF(HOUR, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) as hours_since_last_update
                FROM {table_name}
            """).first()
            
            print(f"\n📋 {source_name}:")
            print(f"  Table: {table_name}")
            print(f"  Records: {result.record_count:,}")
            print(f"  Latest: {result.latest_ingestion}")
            print(f"  Hours since update: {result.hours_since_last_update}")
            
            # Freshness alert
            if result.hours_since_last_update and result.hours_since_last_update > 24:
                print(f"  ⚠ WARNING: Data is stale (> 24 hours)")
            else:
                print(f"  ✓ Data is fresh")
        else:
            print(f"\n⚠ {source_name}: Table does not exist yet - {table_name}")
    except Exception as e:
        print(f"\n❌ {source_name}: Error checking freshness - {e}")

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Ingestion Channel Status Dashboard
# Create dashboard DataFrame
import pandas as pd

print("\n" + "="*80)
print("INGESTION CHANNELS STATUS DASHBOARD")
print("="*80 + "\n")

# Convert to pandas DataFrame for better display
channels_df = pd.DataFrame([
    {
        "Channel": config["name"],
        "Type": config["type"],
        "Source": config["source"],
        "Frequency": config["frequency"],
        "Status": config["status"],
        "Notebook": config["notebook"]
    }
    for key, config in ingestion_channels.items()
])

print(channels_df.to_string(index=False))

print("\n" + "="*80)
print(f"\nSummary:")
print(f"  Total Channels: {len(ingestion_channels)}")
print(f"  Active: {sum(1 for c in ingestion_channels.values() if c['status'] == 'Active')}")
print(f"  Configured: {sum(1 for c in ingestion_channels.values() if c['status'] == 'Configured')}")
print(f"  Streaming: {sum(1 for c in ingestion_channels.values() if c['type'] == 'Streaming')}")
print(f"  Batch: {sum(1 for c in ingestion_channels.values() if c['type'] == 'Batch')}")
print(f"  Event-driven: {sum(1 for c in ingestion_channels.values() if c['type'] == 'Event-driven')}")

# COMMAND ----------

# DBTITLE 1,Volume Storage Check
# Check storage usage in volumes
print("\n" + "="*80)
print("VOLUME STORAGE UTILIZATION")
print("="*80)

import os

def get_directory_size(path):
    """Calculate total size of directory in MB"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception as e:
        return 0
    return total_size / (1024 * 1024)  # Convert to MB

volume_paths = [
    ("/Volumes/financial_lakehouse/raw/landing_files", "Landing Files"),
    ("/Volumes/financial_lakehouse/raw/checkpoints", "Checkpoints"),
    ("/Volumes/financial_lakehouse/raw/dms_cdc", "DMS CDC")
]

for path, name in volume_paths:
    if os.path.exists(path):
        size_mb = get_directory_size(path)
        print(f"\n📁 {name}:")
        print(f"  Path: {path}")
        print(f"  Size: {size_mb:.2f} MB")
    else:
        print(f"\n⚠ {name}: Path does not exist - {path}")

print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Streaming Queries Monitor
# Monitor active streaming queries
print("\n" + "="*80)
print("ACTIVE STREAMING QUERIES")
print("="*80)

active_streams = spark.streams.active

if len(active_streams) > 0:
    print(f"\nFound {len(active_streams)} active stream(s):\n")
    
    for stream in active_streams:
        print(f"▶ Stream: {stream.name if stream.name else stream.id}")
        print(f"  ID: {stream.id}")
        print(f"  Status: {stream.status}")
        
        if stream.recentProgress and len(stream.recentProgress) > 0:
            latest = stream.recentProgress[-1]
            print(f"  Latest Batch: {latest.get('batchId', 'N/A')}")
            print(f"  Input Rows: {latest.get('numInputRows', 0):,}")
            print(f"  Processing Rate: {latest.get('processedRowsPerSecond', 0):.2f} rows/sec")
        print()
else:
    print("\n⚠ No active streaming queries found")
    print("   Start streaming notebooks to begin continuous ingestion")

print("="*80)

# COMMAND ----------

