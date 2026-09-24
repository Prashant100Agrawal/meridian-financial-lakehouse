# Databricks notebook source
# DBTITLE 1,Silver SCD Type 2
# MAGIC %md
# MAGIC # Silver Layer - SCD Type 2 Implementation
# MAGIC ## Slowly Changing Dimensions with Full History
# MAGIC
# MAGIC Implements SCD Type 2 pattern for tracking historical changes:
# MAGIC * **Full History** - Keep all versions of changed records
# MAGIC * **Effective Dating** - Track when each version was valid
# MAGIC * **Current Flag** - Easy filtering for current records
# MAGIC * **Surrogate Keys** - Unique ID for each version

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, date

# COMMAND ----------

# DBTITLE 1,SCD2 Overview
# MAGIC %md
# MAGIC ## SCD Type 2 Pattern
# MAGIC
# MAGIC **Key Columns:**
# MAGIC * `{business_key}` - Natural business key (e.g., account_id)
# MAGIC * `surrogate_key` - Unique ID for each record version  
# MAGIC * `effective_start_date` - When this version became active
# MAGIC * `effective_end_date` - When this version was superseded (NULL = current)
# MAGIC * `is_current` - Boolean flag for current record
# MAGIC * `record_hash` - Hash of attribute values to detect changes

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define tables for SCD Type 2 processing

# COMMAND ----------

# DBTITLE 1,SCD2 Config
# SCD Type 2 table configurations
scd2_tables = {
    "accounts": {
        "source": "financial_lakehouse.silver.accounts",  # From CDC processing
        "target": "financial_lakehouse.silver.accounts_scd2",
        "business_key": ["account_id"],
        "tracked_columns": ["account_name", "account_type", "status", "balance", "owner"],
        "exclude_from_hash": ["silver_insert_timestamp", "silver_update_timestamp"]
    },
    "customers": {
        "source": "financial_lakehouse.silver.customers",
        "target": "financial_lakehouse.silver.customers_scd2",
        "business_key": ["customer_id"],
        "tracked_columns": ["customer_name", "email", "phone", "address", "risk_rating"],
        "exclude_from_hash": ["silver_insert_timestamp", "silver_update_timestamp"]
    }
}

print("✓ SCD2 configurations loaded")

# COMMAND ----------

# DBTITLE 1,SCD2 Functions
# MAGIC %md
# MAGIC ## SCD2 Helper Functions
# MAGIC Utilities for hash generation and SCD2 logic

# COMMAND ----------

# DBTITLE 1,Helper Functions
def generate_hash(df, columns_to_hash):
    """
    Generate MD5 hash of specified columns to detect changes.
    """
    concat_cols = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns_to_hash])
    return df.withColumn("record_hash", md5(concat_cols))

def generate_surrogate_key(df):
    """
    Generate unique surrogate key for each record.
    """
    return df.withColumn("surrogate_key", 
                        concat(col("business_key_value"), 
                               lit("_"), 
                               col("effective_start_date").cast("string")))

print("✓ Helper functions defined")

# COMMAND ----------

# DBTITLE 1,SCD2 Processing
# MAGIC %md
# MAGIC ## SCD2 Processing Function
# MAGIC Implements full SCD Type 2 logic with MERGE

# COMMAND ----------

# DBTITLE 1,Process SCD2
def process_scd2(table_name, config):
    """
    Process SCD Type 2 updates for a dimension table.
    
    Steps:
    1. Read source (current state)
    2. Compare with target (historical state)
    3. Close out changed records (set end_date, is_current=False)
    4. Insert new versions of changed records
    5. Insert new records
    """
    source_table = config["source"]
    target_table = config["target"]
    business_key = config["business_key"]
    tracked_columns = config["tracked_columns"]
    exclude_from_hash = config.get("exclude_from_hash", [])
    
    print(f"\n{'='*70}")
    print(f"Processing SCD Type 2: {table_name}")
    print(f"{'='*70}")
    
    # Read source data
    source_df = spark.table(source_table)
    
    # Columns to hash (all except excluded)
    hash_columns = [c for c in source_df.columns 
                    if c not in business_key and c not in exclude_from_hash]
    
    # Add hash and business key concatenation
    source_with_hash = generate_hash(source_df, hash_columns)
    source_with_hash = source_with_hash.withColumn(
        "business_key_value",
        concat_ws("|", *[col(k) for k in business_key])
    )
    
    # Check if target exists
    if not spark.catalog.tableExists(target_table):
        print(f"  Creating new SCD2 table: {target_table}")
        
        # Initial load - all records are current
        initial_load = source_with_hash \
            .withColumn("effective_start_date", current_date()) \
            .withColumn("effective_end_date", lit(None).cast(DateType())) \
            .withColumn("is_current", lit(True))
        
        initial_load = generate_surrogate_key(initial_load)
        
        initial_load.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        print(f"  ✓ Initial load complete: {initial_load.count():,} records")
        return
    
    # Load existing SCD2 table
    target_df = spark.table(target_table)
    current_records = target_df.filter(col("is_current") == True)
    
    print(f"  Source records: {source_with_hash.count():,}")
    print(f"  Current target records: {current_records.count():,}")
    
    # Join source with current target to find changes
    comparison = source_with_hash.alias("src").join(
        current_records.alias("tgt"),
        [col(f"src.{k}") == col(f"tgt.{k}") for k in business_key],
        "full_outer"
    )
    
    # Identify change types
    changes = comparison.withColumn(
        "change_type",
        when(col("tgt.business_key_value").isNull(), lit("INSERT"))  # New record
        .when(col("src.business_key_value").isNull(), lit("DELETE"))  # Deleted record
        .when(col("src.record_hash") != col("tgt.record_hash"), lit("UPDATE"))  # Changed record
        .otherwise(lit("NO_CHANGE"))  # Unchanged record
    )
    
    # Count changes
    change_counts = changes.groupBy("change_type").count().collect()
    for row in change_counts:
        print(f"  {row['change_type']}: {row['count']:,} records")
    
    # Get records that changed or are new
    changed_keys = changes.filter(
        col("change_type").isin(["INSERT", "UPDATE"])
    ).select(
        *[col(f"src.{k}").alias(k) for k in business_key]
    ).distinct()
    
    if changed_keys.count() == 0:
        print(f"  ✓ No changes detected - skipping")
        return
    
    # Close out old versions (set end_date and is_current=False)
    target_delta = DeltaTable.forName(spark, target_table)
    
    # Build condition for matching keys
    match_condition = " OR ".join([
        "(" + " AND ".join([f"target.{k} = updates.{k}" for k in business_key]) + ")"
    ])
    
    target_delta.alias("target").merge(
        changed_keys.alias("updates"),
        f"({match_condition}) AND target.is_current = true"
    ).whenMatchedUpdate(
        set = {
            "effective_end_date": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    print(f"  ✓ Closed {changed_keys.count():,} old versions")
    
    # Insert new versions
    new_versions = source_with_hash.join(
        changed_keys,
        business_key,
        "inner"
    ).withColumn("effective_start_date", current_date()) \
     .withColumn("effective_end_date", lit(None).cast(DateType())) \
     .withColumn("is_current", lit(True))
    
    new_versions = generate_surrogate_key(new_versions)
    
    new_versions.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)
    
    print(f"  ✓ Inserted {new_versions.count():,} new versions")
    print(f"  ✓ SCD2 processing complete for {table_name}")
    print(f"{'='*70}\n")

print("✓ SCD2 processing function defined")

# COMMAND ----------

# DBTITLE 1,Run SCD2
# MAGIC %md
# MAGIC ## Execute SCD2 Processing
# MAGIC Process all configured SCD Type 2 tables

# COMMAND ----------

# DBTITLE 1,Process All Tables
# Process all SCD2 tables
for table_name, config in scd2_tables.items():
    try:
        # Check if source exists
        if spark.catalog.tableExists(config["source"]):
            process_scd2(table_name, config)
        else:
            print(f"\n⚠ Skipping {table_name}: Source table {config['source']} does not exist")
    except Exception as e:
        print(f"\n⚠ Error processing {table_name}: {e}")
        import traceback
        traceback.print_exc()

print("\n✓ All SCD2 processing complete!")

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## Verification
# MAGIC View SCD2 tables and history

# COMMAND ----------

# DBTITLE 1,Verify SCD2 Tables
print("\n" + "="*70)
print("SCD2 TABLE VERIFICATION")
print("="*70)

for table_name, config in scd2_tables.items():
    target_table = config["target"]
    
    try:
        if spark.catalog.tableExists(target_table):
            # Overall stats
            stats = spark.sql(f"""
                SELECT 
                    COUNT(*) as total_versions,
                    COUNT(DISTINCT business_key_value) as unique_keys,
                    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
                    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) as historical_records
                FROM {target_table}
            """).collect()[0]
            
            print(f"\n✓ {table_name.upper()}:")
            print(f"  Table: {target_table}")
            print(f"  Total Versions: {stats['total_versions']:,}")
            print(f"  Unique Keys: {stats['unique_keys']:,}")
            print(f"  Current Records: {stats['current_records']:,}")
            print(f"  Historical Records: {stats['historical_records']:,}")
            
            # Show example of history tracking
            print(f"\n  Example: History for first key")
            spark.sql(f"""
                SELECT 
                    business_key_value,
                    effective_start_date,
                    effective_end_date,
                    is_current,
                    record_hash
                FROM {target_table}
                WHERE business_key_value = (
                    SELECT business_key_value 
                    FROM {target_table} 
                    LIMIT 1
                )
                ORDER BY effective_start_date
            """).show(truncate=False)
        else:
            print(f"\n⚠ {table_name}: Table does not exist yet")
    except Exception as e:
        print(f"\n⚠ {table_name}: Error - {e}")
    
    print("-" * 70)

# COMMAND ----------

# DBTITLE 1,Query Patterns
# MAGIC %md
# MAGIC ## Common SCD2 Query Patterns
# MAGIC Useful queries for working with SCD Type 2 tables

# COMMAND ----------

# DBTITLE 1,Query Examples
# Example queries for SCD Type 2 tables

print("\n" + "="*70)
print("SCD2 QUERY PATTERNS")
print("="*70)

# Pattern 1: Get current records only
print("\n1. Current Records Only:")
print("   SELECT * FROM table WHERE is_current = true")

# Pattern 2: Get all history for a key
print("\n2. Full History for a Key:")
print("   SELECT * FROM table WHERE account_id = '12345' ORDER BY effective_start_date")

# Pattern 3: Point-in-time query (as of specific date)
print("\n3. Point-in-Time Query:")
print("""   SELECT * FROM table 
   WHERE '2024-01-15' BETWEEN effective_start_date AND COALESCE(effective_end_date, '9999-12-31')""")

# Pattern 4: Find records that changed in a date range
print("\n4. Changes in Date Range:")
print("""   SELECT * FROM table 
   WHERE effective_start_date BETWEEN '2024-01-01' AND '2024-01-31'
   AND is_current = false  -- Show what changed""")

# Pattern 5: Count versions per key
print("\n5. Keys with Most Changes:")
print("""   SELECT business_key_value, COUNT(*) as version_count
   FROM table
   GROUP BY business_key_value
   ORDER BY version_count DESC""")

print("\n" + "="*70)