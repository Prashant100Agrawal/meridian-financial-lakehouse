# Databricks notebook source
# DBTITLE 1,Enhanced Auto Loader Ingestion
# MAGIC %md
# MAGIC # Enhanced Auto Loader Ingestion
# MAGIC ## Advanced cloud file ingestion with Auto Loader (cloudFiles)
# MAGIC
# MAGIC Features:
# MAGIC * **Schema Evolution** - Automatic schema inference and evolution
# MAGIC * **Rescue Data** - Capture malformed records
# MAGIC * **Multiple Formats** - JSON, CSV, Parquet, Avro support
# MAGIC * **Event-driven** - File notifications for instant processing
# MAGIC * **Exactly-once** - Idempotent processing with checkpointing

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define source paths, checkpoints, and target tables for different data sources

# COMMAND ----------

# DBTITLE 1,Source Configurations
# Base paths
raw_volume = "/Volumes/financial_lakehouse/raw"
landing_base = f"{raw_volume}/landing_files"
checkpoint_base = f"{raw_volume}/checkpoints"

# Source configurations for different systems
source_configs = {
    "vendor_files": {
        "path": f"{landing_base}/vendor_files",
        "format": "csv",
        "target": "financial_lakehouse.operational.vendor_files_stream",
        "checkpoint": f"{checkpoint_base}/vendor_files"
    },
    "custody_reports": {
        "path": f"{landing_base}/custody_reports",
        "format": "json",
        "target": "financial_lakehouse.operational.custody_reports_stream",
        "checkpoint": f"{checkpoint_base}/custody_reports"
    },
    "trade_confirmations": {
        "path": f"{landing_base}/trade_confirmations",
        "format": "parquet",
        "target": "financial_lakehouse.operational.trade_confirmations_stream",
        "checkpoint": f"{checkpoint_base}/trade_confirmations"
    }
}

print("✓ Source configurations loaded")
for name, config in source_configs.items():
    print(f"  - {name}: {config['format'].upper()} from {config['path']}")

# COMMAND ----------

# DBTITLE 1,Auto Loader Function
# MAGIC %md
# MAGIC ## Auto Loader Function
# MAGIC Reusable function for creating Auto Loader streams with advanced features

# COMMAND ----------

# DBTITLE 1,Create Auto Loader Stream
def create_autoloader_stream(
    source_path,
    checkpoint_path,
    file_format,
    target_table,
    schema_hints=None,
    additional_options=None
):
    """
    Create an Auto Loader streaming DataFrame with advanced features.
    
    Args:
        source_path: Cloud storage path to monitor
        checkpoint_path: Checkpoint location for state
        file_format: File format (json, csv, parquet, avro)
        target_table: Target Delta table name
        schema_hints: Optional dict of column_name: data_type hints
        additional_options: Optional dict of format-specific options
    
    Returns:
        Configured streaming DataFrame
    """
    # Base Auto Loader options
    reader = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.schemaLocation", checkpoint_path) \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .option("cloudFiles.useNotifications", "true") \
        .option("rescuedDataColumn", "_rescued_data")
    
    # Add schema hints if provided
    if schema_hints:
        for col_name, col_type in schema_hints.items():
            reader = reader.option(f"cloudFiles.schemaHints", f"{col_name} {col_type}")
    
    # Format-specific options
    if file_format == "csv":
        reader = reader \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("mode", "PERMISSIVE")
    elif file_format == "json":
        reader = reader \
            .option("multiLine", "false") \
            .option("mode", "PERMISSIVE")
    
    # Add any additional custom options
    if additional_options:
        for key, value in additional_options.items():
            reader = reader.option(key, value)
    
    # Load the stream
    df = reader.load(source_path)
    
    # Add metadata columns
    df = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("file_path", input_file_name()) \
        .withColumn("file_modification_time", col("_metadata.file_modification_time")) \
        .withColumn("processing_date", current_date())
    
    return df

print("✓ Auto Loader function defined")

# COMMAND ----------

# DBTITLE 1,CSV Example
# MAGIC %md
# MAGIC ## Example 1: CSV Vendor Files
# MAGIC Ingest CSV vendor files with schema hints

# COMMAND ----------

# DBTITLE 1,Load CSV Vendor Files
# Schema hints for CSV files
vendor_schema_hints = {
    "position_date": "DATE",
    "account_id": "STRING",
    "security_id": "STRING",
    "quantity": "DECIMAL(18,4)",
    "market_value": "DECIMAL(18,2)"
}

# Create Auto Loader stream for vendor files
config = source_configs["vendor_files"]

vendor_stream = create_autoloader_stream(
    source_path=config["path"],
    checkpoint_path=config["checkpoint"],
    file_format=config["format"],
    target_table=config["target"],
    schema_hints=vendor_schema_hints,
    additional_options={"delimiter": ",", "dateFormat": "yyyy-MM-dd"}
)

print(f"✓ Vendor files stream created")
print(f"  Source: {config['path']}")
print(f"  Target: {config['target']}")

# COMMAND ----------

# DBTITLE 1,JSON Example
# MAGIC %md
# MAGIC ## Example 2: JSON Custody Reports
# MAGIC Ingest nested JSON custody reports with automatic schema inference

# COMMAND ----------

# DBTITLE 1,Load JSON Custody Reports
# Create Auto Loader stream for JSON custody reports
config = source_configs["custody_reports"]

custody_stream = create_autoloader_stream(
    source_path=config["path"],
    checkpoint_path=config["checkpoint"],
    file_format=config["format"],
    target_table=config["target"]
)

# Flatten nested JSON if needed
custody_stream_flat = custody_stream \
    .select(
        col("account.*"),
        col("positions.*"),
        col("ingestion_timestamp"),
        col("file_path"),
        col("_rescued_data")
    )

print(f"✓ Custody reports stream created")
print(f"  Source: {config['path']}")
print(f"  Target: {config['target']}")

# COMMAND ----------

# DBTITLE 1,Write to Delta
# MAGIC %md
# MAGIC ## Write Streams to Delta Tables
# MAGIC Write all streams to bronze layer with checkpointing

# COMMAND ----------

# DBTITLE 1,Stream Writers
# Write vendor files stream
vendor_query = vendor_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{source_configs['vendor_files']['checkpoint']}_writer") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="1 minute") \
    .toTable(source_configs["vendor_files"]["target"])

print(f"✓ Vendor files streaming to: {source_configs['vendor_files']['target']}")

# Write custody reports stream
custody_query = custody_stream_flat.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{source_configs['custody_reports']['checkpoint']}_writer") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="1 minute") \
    .toTable(source_configs["custody_reports"]["target"])

print(f"✓ Custody reports streaming to: {source_configs['custody_reports']['target']}")

# COMMAND ----------

# DBTITLE 1,Monitor Streams
# MAGIC %md
# MAGIC ## Monitor All Active Streams
# MAGIC Check health, metrics, and performance of all streaming queries

# COMMAND ----------

# DBTITLE 1,Stream Monitoring
import time

print("\n" + "="*70)
print("AUTO LOADER STREAMING QUERIES MONITOR")
print("="*70)

for stream in spark.streams.active:
    print(f"\n▶ Stream ID: {stream.id}")
    print(f"  Status: {stream.status}")
    
    if stream.recentProgress:
        latest = stream.recentProgress[-1]
        print(f"\n  ℹ Latest Batch:")
        print(f"    Batch ID: {latest.get('batchId', 'N/A')}")
        print(f"    Input Rows: {latest.get('numInputRows', 0):,}")
        print(f"    Processing Rate: {latest.get('processedRowsPerSecond', 0):.2f} rows/sec")
        print(f"    Batch Duration: {latest.get('batchDuration', 0)} ms")
        
        sources = latest.get('sources', [])
        if sources:
            print(f"\n  ℹ Source Details:")
            for source in sources:
                print(f"    Files Processed: {source.get('numInputRows', 0)}")
                print(f"    Start Offset: {source.get('startOffset', 'N/A')}")
    print("-" * 70)

print(f"\n✓ Total active streams: {len(spark.streams.active)}")

# COMMAND ----------

# DBTITLE 1,Rescue Data Check
# MAGIC %md
# MAGIC ## Check for Malformed Records
# MAGIC Query rescue data to identify and fix data quality issues

# COMMAND ----------

# DBTITLE 1,Query Rescue Data
# Check for rescued (malformed) data in vendor files
rescued_data_query = f"""
SELECT 
    file_path,
    _rescued_data,
    ingestion_timestamp,
    COUNT(*) as malformed_count
FROM {source_configs['vendor_files']['target']}
WHERE _rescued_data IS NOT NULL
GROUP BY file_path, _rescued_data, ingestion_timestamp
ORDER BY ingestion_timestamp DESC
LIMIT 10
"""

print("⚠ Checking for malformed records...")
try:
    rescued_df = spark.sql(rescued_data_query)
    rescued_count = rescued_df.count()
    
    if rescued_count > 0:
        print(f"\n⚠ Found {rescued_count} groups of malformed records:")
        rescued_df.show(truncate=False)
    else:
        print("✓ No malformed records found - all data parsed successfully")
except Exception as e:
    print(f"  (Table may not exist yet: {e})")

# COMMAND ----------

# DBTITLE 1,Stop Streams
# MAGIC %md
# MAGIC ## Stop Streaming (Optional)
# MAGIC Stop all active Auto Loader streams

# COMMAND ----------

# DBTITLE 1,Stop All Streams
# Uncomment to stop all streams
# for stream in spark.streams.active:
#     print(f"Stopping stream: {stream.id}")
#     stream.stop()
#     print("✓ Stopped")