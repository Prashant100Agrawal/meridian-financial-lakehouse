# Databricks notebook source
# DBTITLE 1,DMS CDC Ingestion
# MAGIC %md
# MAGIC # AWS DMS CDC Ingestion
# MAGIC ## Change Data Capture from Database Sources
# MAGIC
# MAGIC Ingests incremental database changes from AWS DMS (Database Migration Service) for:
# MAGIC * **Full Load** - Initial bulk data load
# MAGIC * **CDC** - Ongoing incremental changes (INSERT, UPDATE, DELETE)
# MAGIC * **Automatic Merge** - Apply changes to Delta tables

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC DMS outputs CDC data to S3 in Parquet format with operation metadata

# COMMAND ----------

# DBTITLE 1,DMS Paths
# DMS output locations (S3 buckets synced to Unity Catalog Volumes)
dms_base_path = "/Volumes/financial_lakehouse/raw/dms_cdc"

# Source table configurations
source_tables = {
    "accounts": {
        "dms_path": f"{dms_base_path}/accounts",
        "target_table": "financial_lakehouse.operational.accounts",
        "primary_keys": ["account_id"],
        "checkpoint": f"{dms_base_path}/checkpoints/accounts"
    },
    "transactions": {
        "dms_path": f"{dms_base_path}/transactions",
        "target_table": "financial_lakehouse.operational.transactions",
        "primary_keys": ["transaction_id"],
        "checkpoint": f"{dms_base_path}/checkpoints/transactions"
    },
    "positions": {
        "dms_path": f"{dms_base_path}/positions",
        "target_table": "financial_lakehouse.operational.positions",
        "primary_keys": ["position_id"],
        "checkpoint": f"{dms_base_path}/checkpoints/positions"
    }
}

print("✓ DMS CDC configurations loaded")

# COMMAND ----------

# DBTITLE 1,CDC Function
# MAGIC %md
# MAGIC ## CDC Processing Function
# MAGIC Reads DMS CDC files and applies changes to Delta tables using MERGE

# COMMAND ----------

# DBTITLE 1,Apply CDC Function
def apply_dms_cdc(table_config):
    """
    Apply DMS CDC changes to a Delta table.
    
    DMS CDC columns:
    - Op: Operation type (I=Insert, U=Update, D=Delete)
    - All source table columns
    """
    dms_path = table_config["dms_path"]
    target_table = table_config["target_table"]
    primary_keys = table_config["primary_keys"]
    checkpoint_path = table_config["checkpoint"]
    
    print(f"\nProcessing CDC for: {target_table}")
    print(f"  Source: {dms_path}")
    
    # Read CDC stream with Auto Loader
    cdc_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", checkpoint_path) \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load(dms_path)
    
    # Add processing metadata
    cdc_df = cdc_df \
        .withColumn("cdc_timestamp", current_timestamp()) \
        .withColumn("cdc_file", input_file_name())
    
    # Define merge logic
    def merge_cdc_batch(batch_df, batch_id):
        """
        Merge each micro-batch into the target Delta table.
        """
        if batch_df.count() == 0:
            return
        
        # Ensure target table exists
        if not spark.catalog.tableExists(target_table):
            # Create table from first batch
            batch_df.filter(col("Op") == "I") \
                .drop("Op", "cdc_timestamp", "cdc_file") \
                .write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(target_table)
            print(f"  ✓ Created table: {target_table}")
            return
        
        # Load target table
        target = DeltaTable.forName(spark, target_table)
        
        # Separate operations
        inserts = batch_df.filter(col("Op") == "I").drop("Op", "cdc_timestamp", "cdc_file")
        updates = batch_df.filter(col("Op") == "U").drop("Op", "cdc_timestamp", "cdc_file")
        deletes = batch_df.filter(col("Op") == "D").drop("Op", "cdc_timestamp", "cdc_file")
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        
        # Process deletes
        if deletes.count() > 0:
            deletes.createOrReplaceTempView("deletes_temp")
            target.alias("target") \
                .merge(
                    deletes.alias("source"),
                    merge_condition
                ) \
                .whenMatchedDelete() \
                .execute()
            print(f"  ✓ Deleted {deletes.count()} records")
        
        # Process updates and inserts
        upserts = inserts.union(updates) if updates.count() > 0 else inserts
        
        if upserts.count() > 0:
            target.alias("target") \
                .merge(
                    upserts.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            print(f"  ✓ Upserted {upserts.count()} records (batch {batch_id})")
    
    # Start streaming query with merge
    query = cdc_df.writeStream \
        .foreachBatch(merge_cdc_batch) \
        .option("checkpointLocation", f"{checkpoint_path}_stream") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

print("✓ CDC function defined")

# COMMAND ----------

# DBTITLE 1,Apply CDC
# MAGIC %md
# MAGIC ## Start CDC Processing
# MAGIC Apply CDC for all configured tables

# COMMAND ----------

# DBTITLE 1,Start CDC Streams
# Start CDC processing for each table
active_queries = []

for table_name, config in source_tables.items():
    try:
        query = apply_dms_cdc(config)
        active_queries.append((table_name, query))
        print(f"✓ Started CDC stream for {table_name}")
    except Exception as e:
        print(f"⚠ Error starting CDC for {table_name}: {e}")

print(f"\n✓ Total active CDC streams: {len(active_queries)}")

# COMMAND ----------

# DBTITLE 1,Monitor CDC
# MAGIC %md
# MAGIC ## Monitor CDC Streams
# MAGIC Check processing status and metrics

# COMMAND ----------

# DBTITLE 1,CDC Monitoring
import time

print("\n" + "="*70)
print("DMS CDC STREAMS MONITOR")
print("="*70)

for stream in spark.streams.active:
    print(f"\n▶ Stream: {stream.name if stream.name else stream.id}")
    print(f"  Status: {stream.status}")
    
    if stream.recentProgress:
        latest = stream.recentProgress[-1]
        print(f"\n  Latest Batch:")
        print(f"    Batch ID: {latest.get('batchId', 'N/A')}")
        print(f"    Input Rows: {latest.get('numInputRows', 0):,}")
        print(f"    Duration: {latest.get('batchDuration', 0)} ms")
    print("-" * 70)

print(f"\n✓ Total active CDC streams: {len(spark.streams.active)}")

# COMMAND ----------

# DBTITLE 1,Verify Tables
# MAGIC %md
# MAGIC ## Verify CDC Results
# MAGIC Check record counts and recent changes in target tables

# COMMAND ----------

# DBTITLE 1,Table Verification
print("\n" + "="*70)
print("TABLE VERIFICATION")
print("="*70)

for table_name, config in source_tables.items():
    target_table = config["target_table"]
    
    try:
        if spark.catalog.tableExists(target_table):
            count = spark.table(target_table).count()
            print(f"\n✓ {table_name.upper()}:")
            print(f"  Table: {target_table}")
            print(f"  Total Records: {count:,}")
            
            # Show sample
            print(f"\n  Recent Records:")
            spark.table(target_table) \
                .orderBy(col(config["primary_keys"][0]).desc()) \
                .limit(3) \
                .show(truncate=False)
        else:
            print(f"\n⚠ {table_name}: Table not yet created")
    except Exception as e:
        print(f"\n⚠ {table_name}: Error - {e}")
    
    print("-" * 70)

# COMMAND ----------

# DBTITLE 1,Stop Streams
# MAGIC %md
# MAGIC ## Stop CDC Streams (Optional)

# COMMAND ----------

# DBTITLE 1,Stop CDC
# Uncomment to stop all CDC streams
# for stream in spark.streams.active:
#     print(f"Stopping: {stream.id}")
#     stream.stop()
#     print("✓ Stopped")