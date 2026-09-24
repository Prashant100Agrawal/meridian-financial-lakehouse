# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Lifecycle Management
# MAGIC ## Manage bronze table retention, compaction, and optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Features
# MAGIC * Automated VACUUM operations
# MAGIC * OPTIMIZE and Z-ORDER
# MAGIC * Data retention policies
# MAGIC * Table statistics updates

# COMMAND ----------

from delta.tables import DeltaTable
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Table Lifecycle Config

# COMMAND ----------

bronze_lifecycle_config = {
    'trading_systems': {
        'retention_days': 30,  # Keep 30 days in bronze
        'optimize_frequency_days': 7,
        'vacuum_retention_hours': 168,  # 7 days
        'zorder_columns': ['trade_date', 'source_system']
    },
    'market_data': {
        'retention_days': 90,
        'optimize_frequency_days': 7,
        'vacuum_retention_hours': 168,
        'zorder_columns': ['date', 'symbol']
    },
    'api_ingestion_logs': {
        'retention_days': 30,
        'optimize_frequency_days': 30,
        'vacuum_retention_hours': 72,  # 3 days
        'zorder_columns': ['ingestion_date']
    }
}

print("📋 Bronze Lifecycle Policies:")
for table, config in bronze_lifecycle_config.items():
    print(f"   {table}:")
    print(f"      Retention: {config['retention_days']} days")
    print(f"      Optimize: Every {config['optimize_frequency_days']} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Optimize and Z-ORDER

# COMMAND ----------

def optimize_bronze_table(table_name, zorder_columns=None):
    """
    Run OPTIMIZE and optionally Z-ORDER on bronze table
    """
    print(f"🔧 Optimizing {table_name}...")
    
    # Basic OPTIMIZE
    optimize_cmd = f"OPTIMIZE {table_name}"
    
    # Add Z-ORDER if columns specified
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        optimize_cmd += f" ZORDER BY ({zorder_clause})"
        print(f"   Z-ORDER columns: {zorder_clause}")
    
    spark.sql(optimize_cmd)
    print(f"   ✅ Optimization complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VACUUM Old Files

# COMMAND ----------

def vacuum_bronze_table(table_name, retention_hours=168):
    """
    Run VACUUM to remove old data files
    
    Args:
        table_name: Full table name
        retention_hours: Retention period (default 7 days)
    """
    print(f"🧹 Vacuuming {table_name} (retention: {retention_hours}h)...")
    
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    print(f"   ✅ VACUUM complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Purge Old Records

# COMMAND ----------

def purge_old_records(table_name, date_column, retention_days):
    """
    Delete records older than retention period
    """
    cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
    
    print(f"🗑️  Purging records from {table_name} older than {cutoff_date}...")
    
    # Count records to delete
    count_query = f"""
        SELECT COUNT(*) as count
        FROM {table_name}
        WHERE {date_column} < '{cutoff_date}'
    """
    records_to_delete = spark.sql(count_query).collect()[0]['count']
    
    if records_to_delete == 0:
        print(f"   ✅ No records to purge")
        return 0
    
    # Delete old records
    delete_query = f"""
        DELETE FROM {table_name}
        WHERE {date_column} < '{cutoff_date}'
    """
    spark.sql(delete_query)
    
    print(f"   ✅ Purged {records_to_delete:,} records")
    return records_to_delete

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Update Table Statistics

# COMMAND ----------

def update_table_statistics(table_name):
    """
    Update table statistics for query optimization
    """
    print(f"📊 Updating statistics for {table_name}...")
    
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
    print(f"   ✅ Statistics updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run Lifecycle Maintenance

# COMMAND ----------

def run_bronze_maintenance(dry_run=True):
    """
    Execute lifecycle maintenance for all bronze tables
    """
    print("="*80)
    print("BRONZE LAYER LIFECYCLE MAINTENANCE")
    print("="*80)
    print(f"Mode: {'DRY RUN' if dry_run else 'PRODUCTION'}\n")
    
    for table_name, config in bronze_lifecycle_config.items():
        full_table_name = f"financial_lakehouse.bronze.{table_name}"
        
        print(f"\n📦 Processing: {full_table_name}")
        print("-"*80)
        
        try:
            # Check if table exists
            if not spark.catalog.tableExists(full_table_name):
                print(f"   ⚠️  Table not found, skipping...")
                continue
            
            # 1. Optimize and Z-ORDER
            if not dry_run:
                optimize_bronze_table(full_table_name, config['zorder_columns'])
            else:
                print(f"   [DRY RUN] Would OPTIMIZE with Z-ORDER: {config['zorder_columns']}")
            
            # 2. Update statistics
            if not dry_run:
                update_table_statistics(full_table_name)
            else:
                print(f"   [DRY RUN] Would update table statistics")
            
            # 3. Purge old records
            if not dry_run:
                purge_old_records(full_table_name, '_bronze_ingestion_timestamp', config['retention_days'])
            else:
                print(f"   [DRY RUN] Would purge records older than {config['retention_days']} days")
            
            # 4. VACUUM old files
            if not dry_run:
                vacuum_bronze_table(full_table_name, config['vacuum_retention_hours'])
            else:
                print(f"   [DRY RUN] Would VACUUM with {config['vacuum_retention_hours']}h retention")
            
            print(f"   ✅ Maintenance complete for {table_name}")
            
        except Exception as e:
            print(f"   ❌ Error processing {table_name}: {str(e)}")
    
    print("\n" + "="*80)
    print("MAINTENANCE COMPLETE")
    print("="*80)

# Run in dry-run mode
run_bronze_maintenance(dry_run=True)

# COMMAND ----------

# To run in production mode, uncomment:
# run_bronze_maintenance(dry_run=False)

# COMMAND ----------

print("✅ Bronze lifecycle management ready")
