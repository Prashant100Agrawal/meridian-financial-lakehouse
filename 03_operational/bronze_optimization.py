# Databricks notebook source
# DBTITLE 1,Bronze Layer Performance Optimization
# MAGIC %md
# MAGIC # Bronze Layer Performance Optimization
# MAGIC ## Delta Lake Optimization, Z-Ordering, and Lifecycle Management
# MAGIC
# MAGIC This notebook implements performance optimization strategies for bronze layer tables:
# MAGIC * **OPTIMIZE** - Compact small files for better read performance
# MAGIC * **Z-Ordering** - Colocate related data for faster queries
# MAGIC * **VACUUM** - Clean up old files to reduce storage costs
# MAGIC * **Lifecycle Management** - Automated retention policies
# MAGIC * **Schema Tracking** - Monitor schema evolution

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import json

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define bronze tables and optimization settings

# COMMAND ----------

# DBTITLE 1,Bronze Tables Config
# Bronze tables to optimize
bronze_tables = [
    {
        "table": "financial_lakehouse.operational.trading_systems",
        "zorder_columns": ["trade_date", "account_id"],
        "retention_days": 90
    },
    {
        "table": "financial_lakehouse.operational.transactions",
        "zorder_columns": ["transaction_date", "account_id"],
        "retention_days": 180
    },
    {
        "table": "financial_lakehouse.operational.positions",
        "zorder_columns": ["position_date", "account_id"],
        "retention_days": 365
    },
    {
        "table": "financial_lakehouse.operational.accounts",
        "zorder_columns": ["account_id"],
        "retention_days": 730  # 2 years for dimensional data
    },
    {
        "table": "financial_lakehouse.operational.custody_platforms",
        "zorder_columns": ["position_date", "account_id"],
        "retention_days": 180
    }
]

print(f"✓ Configured {len(bronze_tables)} tables for optimization")

# COMMAND ----------

# DBTITLE 1,Delta OPTIMIZE
# MAGIC %md
# MAGIC ## Delta OPTIMIZE
# MAGIC Compact small files into larger files for better read performance
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Faster queries (fewer files to scan)
# MAGIC * Better compression
# MAGIC * Reduced metadata overhead
# MAGIC * Improved caching efficiency
# MAGIC
# MAGIC **When to run:** After bulk ingestion or when small files accumulate

# COMMAND ----------

# DBTITLE 1,Optimize Function
def optimize_table(table_name, zorder_columns=None):
    """
    Optimize a Delta table by compacting small files.
    Optionally apply Z-ordering for better data clustering.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        zorder_columns: List of columns to Z-order by (optional)
    """
    print(f"\n{'='*70}")
    print(f"Optimizing: {table_name}")
    print(f"{'='*70}")
    
    try:
        # Get table statistics before optimization
        pre_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
            "numFiles", "sizeInBytes"
        ).collect()[0]
        
        print(f"  Before:")
        print(f"    Files: {pre_stats['numFiles']:,}")
        print(f"    Size: {pre_stats['sizeInBytes'] / (1024**3):.2f} GB")
        
        # Run OPTIMIZE
        if zorder_columns:
            print(f"  Applying Z-ORDER BY: {', '.join(zorder_columns)}")
            spark.sql(f"""
                OPTIMIZE {table_name}
                ZORDER BY ({', '.join(zorder_columns)})
            """)
        else:
            spark.sql(f"OPTIMIZE {table_name}")
        
        # Get table statistics after optimization
        post_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
            "numFiles", "sizeInBytes"
        ).collect()[0]
        
        print(f"  After:")
        print(f"    Files: {post_stats['numFiles']:,}")
        print(f"    Size: {post_stats['sizeInBytes'] / (1024**3):.2f} GB")
        
        # Calculate improvements
        files_reduced = pre_stats['numFiles'] - post_stats['numFiles']
        reduction_pct = (files_reduced / pre_stats['numFiles'] * 100) if pre_stats['numFiles'] > 0 else 0
        
        print(f"  Improvement:")
        print(f"    Files reduced: {files_reduced:,} ({reduction_pct:.1f}%)")
        print(f"  ✓ Optimization complete")
        
        return True
        
    except Exception as e:
        print(f"  ⚠ Error optimizing {table_name}: {e}")
        return False

print("✓ Optimize function defined")

# COMMAND ----------

# DBTITLE 1,Run OPTIMIZE
# MAGIC %md
# MAGIC ## Run OPTIMIZE on All Bronze Tables

# COMMAND ----------

# DBTITLE 1,Execute Optimization
# Optimize all bronze tables
optimization_results = []

for config in bronze_tables:
    table_name = config["table"]
    zorder_cols = config.get("zorder_columns")
    
    # Check if table exists
    if spark.catalog.tableExists(table_name):
        success = optimize_table(table_name, zorder_cols)
        optimization_results.append((table_name, success))
    else:
        print(f"\n⚠ Skipping {table_name}: Table does not exist")
        optimization_results.append((table_name, False))

# Summary
success_count = sum(1 for _, success in optimization_results if success)
print(f"\n" + "="*70)
print(f"OPTIMIZATION SUMMARY")
print(f"="*70)
print(f"Total tables: {len(optimization_results)}")
print(f"Optimized successfully: {success_count}")
print(f"Failed/Skipped: {len(optimization_results) - success_count}")

# COMMAND ----------

# DBTITLE 1,VACUUM
# MAGIC %md
# MAGIC ## VACUUM - Storage Cleanup
# MAGIC Remove old data files no longer referenced by the table
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Reduce storage costs
# MAGIC * Clean up old versions after optimization
# MAGIC * Remove deleted data files
# MAGIC
# MAGIC **Important:** Respects retention period (default 7 days) for time travel

# COMMAND ----------

# DBTITLE 1,Vacuum Function
def vacuum_table(table_name, retention_hours=168):
    """
    Run VACUUM on a Delta table to remove old files.
    
    Args:
        table_name: Full table name
        retention_hours: Retention period in hours (default: 7 days = 168 hours)
    """
    print(f"\n{'='*70}")
    print(f"Vacuuming: {table_name}")
    print(f"{'='*70}")
    
    try:
        # Get table size before vacuum
        pre_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
            "sizeInBytes"
        ).collect()[0]
        
        print(f"  Before:")
        print(f"    Size: {pre_stats['sizeInBytes'] / (1024**3):.2f} GB")
        print(f"  Retention: {retention_hours} hours ({retention_hours/24:.1f} days)")
        
        # Run VACUUM
        spark.sql(f"""
            VACUUM {table_name} RETAIN {retention_hours} HOURS
        """)
        
        # Get table size after vacuum
        post_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
            "sizeInBytes"
        ).collect()[0]
        
        print(f"  After:")
        print(f"    Size: {post_stats['sizeInBytes'] / (1024**3):.2f} GB")
        
        space_freed = (pre_stats['sizeInBytes'] - post_stats['sizeInBytes']) / (1024**3)
        print(f"  Space freed: {space_freed:.2f} GB")
        print(f"  ✓ Vacuum complete")
        
        return True
        
    except Exception as e:
        print(f"  ⚠ Error vacuuming {table_name}: {e}")
        return False

print("✓ Vacuum function defined")

# COMMAND ----------

# DBTITLE 1,Run VACUUM
# MAGIC %md
# MAGIC ## Run VACUUM on All Bronze Tables

# COMMAND ----------

# DBTITLE 1,Execute VACUUM
# Vacuum all bronze tables with their configured retention periods
vacuum_results = []

for config in bronze_tables:
    table_name = config["table"]
    retention_days = config.get("retention_days", 90)
    retention_hours = retention_days * 24
    
    if spark.catalog.tableExists(table_name):
        success = vacuum_table(table_name, retention_hours)
        vacuum_results.append((table_name, success))
    else:
        print(f"\n⚠ Skipping {table_name}: Table does not exist")

# Summary
success_count = sum(1 for _, success in vacuum_results if success)
print(f"\n" + "="*70)
print(f"VACUUM SUMMARY")
print(f"="*70)
print(f"Total tables: {len(vacuum_results)}")
print(f"Vacuumed successfully: {success_count}")
print(f"Failed/Skipped: {len(vacuum_results) - success_count}")

# COMMAND ----------

# DBTITLE 1,Table Properties
# MAGIC %md
# MAGIC ## Table Properties & Auto-Optimize
# MAGIC Configure Delta table properties for automatic optimization

# COMMAND ----------

# DBTITLE 1,Set Table Properties
def configure_table_properties(table_name):
    """
    Set recommended Delta table properties for bronze layer.
    """
    print(f"\nConfiguring properties for: {table_name}")
    
    try:
        # Enable auto-optimize (Databricks-specific)
        spark.sql(f"""
            ALTER TABLE {table_name} SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '365 days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.checkpoint.writeStatsAsJson' = 'true',
                'delta.checkpoint.writeStatsAsStruct' = 'true'
            )
        """)
        
        print(f"  ✓ Properties configured:")
        print(f"    - Auto optimize write: Enabled")
        print(f"    - Auto compact: Enabled")
        print(f"    - Change data feed: Enabled")
        print(f"    - Log retention: 365 days")
        print(f"    - File retention: 7 days")
        
        return True
    except Exception as e:
        print(f"  ⚠ Error: {e}")
        return False

# Configure all bronze tables
print("\n" + "="*70)
print("CONFIGURING TABLE PROPERTIES")
print("="*70)

for config in bronze_tables:
    table_name = config["table"]
    if spark.catalog.tableExists(table_name):
        configure_table_properties(table_name)

# COMMAND ----------

# DBTITLE 1,Schema Evolution Tracking
# MAGIC %md
# MAGIC ## Schema Evolution Tracking
# MAGIC Monitor schema changes across all bronze tables

# COMMAND ----------

# DBTITLE 1,Track Schema Changes
# Create schema tracking table if it doesn't exist
schema_tracking_table = "financial_lakehouse.operational.schema_evolution_log"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {schema_tracking_table} (
        table_name STRING,
        schema_json STRING,
        column_count INT,
        column_names ARRAY<STRING>,
        partition_columns ARRAY<STRING>,
        tracked_timestamp TIMESTAMP
    )
    USING DELTA
""")

print(f"✓ Schema tracking table: {schema_tracking_table}")

# COMMAND ----------

# DBTITLE 1,Log Current Schemas
# Log current schema for all bronze tables
print("\n" + "="*70)
print("SCHEMA EVOLUTION TRACKING")
print("="*70)

for config in bronze_tables:
    table_name = config["table"]
    
    if spark.catalog.tableExists(table_name):
        try:
            # Get table schema
            table_df = spark.table(table_name)
            schema_json = table_df.schema.json()
            columns = table_df.columns
            column_count = len(columns)
            
            # Get partition columns
            table_details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            partition_cols = table_details["partitionColumns"] if "partitionColumns" in table_details else []
            
            print(f"\n{table_name}:")
            print(f"  Columns: {column_count}")
            print(f"  Partitions: {partition_cols if partition_cols else 'None'}")
            
            # Log to tracking table
            spark.sql(f"""
                INSERT INTO {schema_tracking_table}
                SELECT
                    '{table_name}' as table_name,
                    '{schema_json.replace("'", "\\'")}'  as schema_json,
                    {column_count} as column_count,
                    array({', '.join([f"'{c}'" for c in columns])}) as column_names,
                    array({', '.join([f"'{c}'" for c in partition_cols]) if partition_cols else ''}) as partition_columns,
                    current_timestamp() as tracked_timestamp
            """)
            
            print(f"  ✓ Schema logged")
            
        except Exception as e:
            print(f"\n⚠ Error tracking {table_name}: {e}")

print(f"\n✓ Schema tracking complete")

# COMMAND ----------

# DBTITLE 1,View Schema History
# MAGIC %md
# MAGIC ## View Schema Evolution History
# MAGIC Query schema changes over time

# COMMAND ----------

# DBTITLE 1,Schema History Query
# View schema evolution history
print("\n" + "="*70)
print("SCHEMA EVOLUTION HISTORY")
print("="*70 + "\n")

try:
    spark.sql(f"""
        SELECT 
            table_name,
            column_count,
            size(column_names) as current_columns,
            tracked_timestamp,
            DATEDIFF(current_timestamp(), tracked_timestamp) as days_ago
        FROM {schema_tracking_table}
        ORDER BY table_name, tracked_timestamp DESC
    """).show(truncate=False)
    
    # Detect schema changes
    print("\nDetecting schema changes...")
    spark.sql(f"""
        WITH ranked AS (
            SELECT 
                table_name,
                column_count,
                tracked_timestamp,
                LAG(column_count) OVER (PARTITION BY table_name ORDER BY tracked_timestamp) as prev_column_count
            FROM {schema_tracking_table}
        )
        SELECT 
            table_name,
            tracked_timestamp as change_date,
            column_count - prev_column_count as columns_changed
        FROM ranked
        WHERE prev_column_count IS NOT NULL
          AND column_count != prev_column_count
        ORDER BY tracked_timestamp DESC
    """).show(truncate=False)
    
except Exception as e:
    print(f"No schema history available yet: {e}")

# COMMAND ----------

# DBTITLE 1,Optimization Statistics
# MAGIC %md
# MAGIC ## Table Statistics & Performance Metrics
# MAGIC View detailed statistics for all bronze tables

# COMMAND ----------

# DBTITLE 1,Table Stats
# Collect and display table statistics
print("\n" + "="*70)
print("BRONZE LAYER TABLE STATISTICS")
print("="*70)

stats_list = []

for config in bronze_tables:
    table_name = config["table"]
    
    if spark.catalog.tableExists(table_name):
        try:
            # Get detailed table info
            details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            
            stats = {
                "table": table_name.split(".")[-1],
                "num_files": details["numFiles"],
                "size_gb": round(details["sizeInBytes"] / (1024**3), 2),
                "rows": spark.table(table_name).count(),
                "partitions": len(details["partitionColumns"]) if details.get("partitionColumns") else 0
            }
            stats_list.append(stats)
            
        except Exception as e:
            print(f"Error getting stats for {table_name}: {e}")

# Display as DataFrame
if stats_list:
    stats_df = spark.createDataFrame(stats_list)
    print("\n")
    stats_df.orderBy("size_gb", ascending=False).show(truncate=False)
    
    # Summary
    total_size = sum(s["size_gb"] for s in stats_list)
    total_files = sum(s["num_files"] for s in stats_list)
    total_rows = sum(s["rows"] for s in stats_list)
    
    print(f"\nTotals:")
    print(f"  Tables: {len(stats_list)}")
    print(f"  Total Size: {total_size:.2f} GB")
    print(f"  Total Files: {total_files:,}")
    print(f"  Total Rows: {total_rows:,}")
else:
    print("\n⚠ No bronze tables found with statistics")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Optimization Summary
# MAGIC
# MAGIC ### What We Did:
# MAGIC 1. ✓ **OPTIMIZE** - Compacted small files for better read performance
# MAGIC 2. ✓ **Z-ORDER** - Colocated related data by key columns
# MAGIC 3. ✓ **VACUUM** - Cleaned up old files with retention policies
# MAGIC 4. ✓ **Table Properties** - Enabled auto-optimization
# MAGIC 5. ✓ **Schema Tracking** - Logged schemas for evolution monitoring
# MAGIC
# MAGIC ### Recommended Schedule:
# MAGIC * **OPTIMIZE**: Run weekly or after bulk ingestion
# MAGIC * **VACUUM**: Run monthly to reclaim storage
# MAGIC * **Schema Tracking**: Run daily or on-demand
# MAGIC * **Table Properties**: Set once, persist automatically
# MAGIC
# MAGIC ### Next Steps:
# MAGIC * Schedule this notebook to run automatically
# MAGIC * Monitor file counts over time
# MAGIC * Adjust Z-ORDER columns based on query patterns
# MAGIC * Review and adjust retention policies