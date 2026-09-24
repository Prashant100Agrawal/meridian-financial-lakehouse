# Databricks notebook source
# DBTITLE 1,Silver CDC MERGE
# MAGIC %md
# MAGIC # Silver Layer - CDC MERGE Operations
# MAGIC ## Idempotent incremental processing with MERGE
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC * **Incremental processing** - Process only new/changed records from bronze
# MAGIC * **Idempotent MERGE** - Can re-run safely without duplicates
# MAGIC * **Change Data Capture** - Track inserts, updates, deletes
# MAGIC * **Watermark tracking** - Resume from last processed point

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define source and target tables for CDC processing

# COMMAND ----------

# DBTITLE 1,CDC Config
# CDC Table Configurations
cdc_tables = {
    "accounts": {
        "source": "financial_lakehouse.bronze.accounts",
        "target": "financial_lakehouse.silver.accounts",
        "primary_keys": ["account_id"],
        "watermark_col": "ingestion_timestamp",
        "watermark_table": "financial_lakehouse.silver.cdc_watermarks"
    },
    "transactions": {
        "source": "financial_lakehouse.bronze.transactions",
        "target": "financial_lakehouse.silver.transactions",
        "primary_keys": ["transaction_id"],
        "watermark_col": "ingestion_timestamp",
        "watermark_table": "financial_lakehouse.silver.cdc_watermarks"
    },
    "positions": {
        "source": "financial_lakehouse.bronze.positions",
        "target": "financial_lakehouse.silver.positions",
        "primary_keys": ["position_id", "position_date"],
        "watermark_col": "ingestion_timestamp",
        "watermark_table": "financial_lakehouse.silver.cdc_watermarks"
    }
}

print("✓ CDC configurations loaded")

# COMMAND ----------

# DBTITLE 1,Watermark Setup
# MAGIC %md
# MAGIC ## Watermark Management
# MAGIC Track last processed timestamp for incremental processing

# COMMAND ----------

# DBTITLE 1,Create Watermark Table
# Create watermark table if it doesn't exist
watermark_table = "financial_lakehouse.silver.cdc_watermarks"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {watermark_table} (
        table_name STRING,
        last_processed_timestamp TIMESTAMP,
        last_processed_date DATE,
        records_processed BIGINT,
        update_timestamp TIMESTAMP
    )
    USING DELTA
""")

print(f"✓ Watermark table ready: {watermark_table}")

# COMMAND ----------

# DBTITLE 1,Watermark Functions
def get_watermark(table_name, watermark_table):
    """
    Get the last processed timestamp for a table.
    Returns None if no watermark exists (first run).
    """
    try:
        result = spark.sql(f"""
            SELECT last_processed_timestamp
            FROM {watermark_table}
            WHERE table_name = '{table_name}'
            ORDER BY update_timestamp DESC
            LIMIT 1
        """)
        
        if result.count() > 0:
            return result.first()["last_processed_timestamp"]
        return None
    except:
        return None

def update_watermark(table_name, new_watermark, records_processed, watermark_table):
    """
    Update the watermark for a table after successful processing.
    """
    spark.sql(f"""
        INSERT INTO {watermark_table}
        VALUES (
            '{table_name}',
            CAST('{new_watermark}' AS TIMESTAMP),
            CAST('{new_watermark}' AS DATE),
            {records_processed},
            current_timestamp()
        )
    """)
    print(f"  ✓ Watermark updated: {new_watermark}")

print("✓ Watermark functions defined")

# COMMAND ----------

# DBTITLE 1,CDC Function
# MAGIC %md
# MAGIC ## CDC MERGE Function
# MAGIC Incremental processing with idempotent MERGE operations

# COMMAND ----------

# DBTITLE 1,Process CDC
def process_cdc_incremental(table_name, config):
    """
    Process incremental changes from bronze to silver using CDC MERGE.
    
    Features:
    - Idempotent: can re-run safely
    - Incremental: only processes new data
    - Efficient: uses watermark for filtering
    """
    source_table = config["source"]
    target_table = config["target"]
    primary_keys = config["primary_keys"]
    watermark_col = config["watermark_col"]
    watermark_table = config["watermark_table"]
    
    print(f"\n{'='*70}")
    print(f"Processing CDC: {table_name}")
    print(f"{'='*70}")
    
    # Get last watermark
    last_watermark = get_watermark(table_name, watermark_table)
    
    if last_watermark:
        print(f"  Last watermark: {last_watermark}")
        # Incremental load
        new_data = spark.table(source_table).filter(
            col(watermark_col) > last_watermark
        )
    else:
        print(f"  First run - loading all data")
        # Full initial load
        new_data = spark.table(source_table)
    
    record_count = new_data.count()
    print(f"  Records to process: {record_count:,}")
    
    if record_count == 0:
        print(f"  ✓ No new records - skipping")
        return
    
    # Get max watermark from new data
    new_watermark = new_data.agg(max(col(watermark_col))).collect()[0][0]
    print(f"  New watermark: {new_watermark}")
    
    # Data quality and cleansing
    cleaned_data = new_data \
        .dropDuplicates(primary_keys) \
        .filter(" AND ".join([f"{pk} IS NOT NULL" for pk in primary_keys]))
    
    # Check if target table exists
    if not spark.catalog.tableExists(target_table):
        print(f"  Creating new table: {target_table}")
        cleaned_data \
            .withColumn("silver_insert_timestamp", current_timestamp()) \
            .withColumn("silver_update_timestamp", current_timestamp()) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
    else:
        # MERGE into existing table
        print(f"  Merging into: {target_table}")
        
        target = DeltaTable.forName(spark, target_table)
        
        # Build merge condition
        merge_condition = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in primary_keys]
        )
        
        # Add update timestamp to source
        source_with_ts = cleaned_data.withColumn(
            "silver_update_timestamp", current_timestamp()
        )
        
        # Execute MERGE
        target.alias("target").merge(
            source_with_ts.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsert(
            values = {
                **{col: f"source.{col}" for col in cleaned_data.columns},
                "silver_insert_timestamp": "current_timestamp()",
                "silver_update_timestamp": "current_timestamp()"
            }
        ).execute()
    
    # Update watermark
    update_watermark(table_name, new_watermark, record_count, watermark_table)
    
    print(f"  ✓ CDC processing complete for {table_name}")
    print(f"{'='*70}\n")

print("✓ CDC processing function defined")

# COMMAND ----------

# DBTITLE 1,Run CDC
# MAGIC %md
# MAGIC ## Execute CDC Processing
# MAGIC Process all configured tables incrementally

# COMMAND ----------

# DBTITLE 1,Process All Tables
# Process CDC for all tables
for table_name, config in cdc_tables.items():
    try:
        process_cdc_incremental(table_name, config)
    except Exception as e:
        print(f"⚠ Error processing {table_name}: {e}")
        import traceback
        traceback.print_exc()

print("\n✓ All CDC processing complete!")

# COMMAND ----------

# DBTITLE 1,Verify Results
# MAGIC %md
# MAGIC ## Verification
# MAGIC Check record counts and data quality in silver tables

# COMMAND ----------

# DBTITLE 1,Verify Tables
print("\n" + "="*70)
print("SILVER TABLE VERIFICATION")
print("="*70)

for table_name, config in cdc_tables.items():
    target_table = config["target"]
    
    try:
        if spark.catalog.tableExists(target_table):
            count = spark.table(target_table).count()
            
            # Get insert/update stats
            stats = spark.sql(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(silver_insert_timestamp) as first_insert,
                    MAX(silver_update_timestamp) as last_update
                FROM {target_table}
            """).collect()[0]
            
            print(f"\n✓ {table_name.upper()}:")
            print(f"  Table: {target_table}")
            print(f"  Total Records: {stats['total_records']:,}")
            print(f"  First Insert: {stats['first_insert']}")
            print(f"  Last Update: {stats['last_update']}")
            
            # Sample records
            print(f"\n  Sample Records:")
            spark.table(target_table).limit(3).show(truncate=False)
        else:
            print(f"\n⚠ {table_name}: Table does not exist yet")
    except Exception as e:
        print(f"\n⚠ {table_name}: Error - {e}")
    
    print("-" * 70)

# COMMAND ----------

# DBTITLE 1,Check Watermarks
# MAGIC %md
# MAGIC ## View Watermarks
# MAGIC Show processing history and watermarks

# COMMAND ----------

# DBTITLE 1,Show Watermarks
# Display watermark history
print("\n" + "="*70)
print("CDC WATERMARK HISTORY")
print("="*70 + "\n")

try:
    spark.sql(f"""
        SELECT 
            table_name,
            last_processed_timestamp,
            records_processed,
            update_timestamp
        FROM {cdc_tables['accounts']['watermark_table']}
        ORDER BY table_name, update_timestamp DESC
    """).show(truncate=False)
except Exception as e:
    print(f"Watermark table is empty or does not exist: {e}")