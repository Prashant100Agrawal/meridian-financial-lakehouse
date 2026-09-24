# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Evolution Handler
# MAGIC ## Automatically detect and handle schema changes in source data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Features
# MAGIC * Auto-detect schema drift
# MAGIC * Column addition handling
# MAGIC * Data type changes
# MAGIC * Column rename detection

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema Comparison

# COMMAND ----------

def compare_schemas(current_schema, new_schema):
    """
    Compare two schemas and detect differences
    
    Returns:
        dict: Schema changes (added, removed, type_changes)
    """
    current_fields = {f.name: f.dataType for f in current_schema.fields}
    new_fields = {f.name: f.dataType for f in new_schema.fields}
    
    added = {k: v for k, v in new_fields.items() if k not in current_fields}
    removed = {k: v for k, v in current_fields.items() if k not in new_fields}
    type_changes = {
        k: {'old': current_fields[k], 'new': new_fields[k]}
        for k in set(current_fields.keys()) & set(new_fields.keys())
        if str(current_fields[k]) != str(new_fields[k])
    }
    
    return {
        'added_columns': added,
        'removed_columns': removed,
        'type_changes': type_changes
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auto Schema Evolution

# COMMAND ----------

def evolve_table_schema(table_name, new_df, merge_schema=True):
    """
    Automatically evolve table schema based on incoming data
    
    Args:
        table_name: Full table name (catalog.schema.table)
        new_df: New DataFrame with potential schema changes
        merge_schema: Enable schema merging
    """
    print(f"🔍 Checking schema evolution for {table_name}")
    
    # Check if table exists
    if spark.catalog.tableExists(table_name):
        current_df = spark.table(table_name)
        
        # Compare schemas
        changes = compare_schemas(current_df.schema, new_df.schema)
        
        if changes['added_columns']:
            print(f"   ✅ New columns detected: {list(changes['added_columns'].keys())}")
        
        if changes['removed_columns']:
            print(f"   ⚠️  Removed columns detected: {list(changes['removed_columns'].keys())}")
            # Fill removed columns with NULL in new data
            for col_name, col_type in changes['removed_columns'].items():
                new_df = new_df.withColumn(col_name, F.lit(None).cast(col_type))
        
        if changes['type_changes']:
            print(f"   ⚠️  Type changes detected: {list(changes['type_changes'].keys())}")
            # Handle type conversions (cast to string for safety)
            for col_name, types in changes['type_changes'].items():
                print(f"      {col_name}: {types['old']} -> {types['new']}")
        
        # Write with schema evolution enabled
        new_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true" if merge_schema else "false") \
            .saveAsTable(table_name)
        
        print(f"   ✅ Schema evolved and data appended")
    else:
        # Create new table
        new_df.write.format("delta").saveAsTable(table_name)
        print(f"   ✅ New table created: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema Change Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS financial_lakehouse.reporting.schema_evolution_log (
# MAGIC   table_name STRING,
# MAGIC   change_type STRING,  -- 'COLUMN_ADDED', 'COLUMN_REMOVED', 'TYPE_CHANGED'
# MAGIC   column_name STRING,
# MAGIC   old_type STRING,
# MAGIC   new_type STRING,
# MAGIC   detected_timestamp TIMESTAMP,
# MAGIC   change_metadata STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Schema evolution audit log';

# COMMAND ----------

def log_schema_change(table_name, change_type, column_name, old_type=None, new_type=None, metadata=None):
    """
    Log schema evolution changes
    """
    log_entry = spark.createDataFrame([{
        'table_name': table_name,
        'change_type': change_type,
        'column_name': column_name,
        'old_type': str(old_type) if old_type else None,
        'new_type': str(new_type) if new_type else None,
        'detected_timestamp': datetime.now(),
        'change_metadata': metadata
    }])
    
    log_entry.write.mode("append").saveAsTable("financial_lakehouse.reporting.schema_evolution_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Example Usage

# COMMAND ----------

# Example: Load new trading data with potential schema changes
# new_trading_data = spark.read.json("/path/to/new/trading/data")
# evolve_table_schema('financial_lakehouse.operational.trading_systems', new_trading_data)

# COMMAND ----------

print("✅ Schema evolution handler ready")
