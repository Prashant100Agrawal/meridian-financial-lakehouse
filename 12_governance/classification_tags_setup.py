# Databricks notebook source
# MAGIC %md
# MAGIC # Classification Tags Setup
# MAGIC ## Automated data classification and tagging for Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tag Categories
# MAGIC * Data Sensitivity (Public, Internal, Confidential, Restricted)
# MAGIC * Data Domain (Trading, Risk, Compliance, Analytics)
# MAGIC * PII Level (None, Low, Medium, High)
# MAGIC * Retention Period (7d, 30d, 1y, 7y, Permanent)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CreateTable, TableInfo
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Tag Definitions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create tags for data sensitivity
# MAGIC CREATE TAG IF NOT EXISTS financial_lakehouse.reporting.data_sensitivity;
# MAGIC CREATE TAG IF NOT EXISTS financial_lakehouse.reporting.data_domain;
# MAGIC CREATE TAG IF NOT EXISTS financial_lakehouse.reporting.pii_level;
# MAGIC CREATE TAG IF NOT EXISTS financial_lakehouse.reporting.retention_period;
# MAGIC CREATE TAG IF NOT EXISTS financial_lakehouse.reporting.data_owner;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Apply Tags to Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag account_performance table
# MAGIC ALTER TABLE financial_lakehouse.reporting.account_performance
# MAGIC SET TAGS (
# MAGIC   'data_sensitivity' = 'Confidential',
# MAGIC   'data_domain' = 'Analytics',
# MAGIC   'pii_level' = 'Medium',
# MAGIC   'retention_period' = '7y',
# MAGIC   'data_owner' = 'Portfolio Management'
# MAGIC );
# MAGIC
# MAGIC -- Tag daily_trading_summary
# MAGIC ALTER TABLE financial_lakehouse.reporting.daily_trading_summary
# MAGIC SET TAGS (
# MAGIC   'data_sensitivity' = 'Internal',
# MAGIC   'data_domain' = 'Trading',
# MAGIC   'pii_level' = 'None',
# MAGIC   'retention_period' = '7y',
# MAGIC   'data_owner' = 'Trading Desk'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Automated Tagging Rules

# COMMAND ----------

# Automated tagging based on table patterns
def auto_tag_tables(catalog='financial_lakehouse', schema='gold'):
    """
    Automatically tag tables based on naming conventions and metadata
    """
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    
    tagging_rules = {
        'account_': {
            'data_sensitivity': 'Confidential',
            'pii_level': 'High',
            'data_domain': 'Customer'
        },
        'trading_': {
            'data_sensitivity': 'Internal',
            'pii_level': 'None',
            'data_domain': 'Trading'
        },
        'risk_': {
            'data_sensitivity': 'Confidential',
            'pii_level': 'Low',
            'data_domain': 'Risk'
        },
        'compliance_': {
            'data_sensitivity': 'Restricted',
            'pii_level': 'Medium',
            'data_domain': 'Compliance'
        }
    }
    
    for table in tables:
        table_name = table['tableName']
        full_name = f"{catalog}.{schema}.{table_name}"
        
        # Match table name pattern
        for pattern, tags in tagging_rules.items():
            if table_name.startswith(pattern):
                tag_clause = ', '.join([f"'{k}' = '{v}'" for k, v in tags.items()])
                sql = f"ALTER TABLE {full_name} SET TAGS ({tag_clause})"
                try:
                    spark.sql(sql)
                    print(f"✅ Tagged {table_name}: {tags}")
                except Exception as e:
                    print(f"❌ Error tagging {table_name}: {e}")
                break

# Run auto-tagging
auto_tag_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tag PII Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Tag specific columns with PII level
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customers
# MAGIC -- ALTER COLUMN email SET TAGS ('pii_level' = 'High', 'data_type' = 'Email');
# MAGIC --
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customers
# MAGIC -- ALTER COLUMN ssn SET TAGS ('pii_level' = 'High', 'data_type' = 'SSN');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query Tagged Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables with Confidential or Restricted data
# MAGIC SELECT 
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE tag_name = 'data_sensitivity'
# MAGIC   AND tag_value IN ('Confidential', 'Restricted')
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables/columns with PII
# MAGIC SELECT 
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE tag_name = 'pii_level'
# MAGIC   AND tag_value != 'None'
# MAGIC ORDER BY tag_value DESC, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tag Audit Report

# COMMAND ----------

# Tag coverage report
def tag_coverage_report(catalog='financial_lakehouse', schema='gold'):
    """
    Generate a report on tag coverage across tables
    """
    # Get all tables
    total_tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").count()
    
    # Get tagged tables
    tagged_tables = spark.sql(f"""
        SELECT COUNT(DISTINCT table_name) as tagged_count
        FROM system.information_schema.table_tags
        WHERE table_catalog = '{catalog}' 
          AND table_schema = '{schema}'
    """).collect()[0]['tagged_count']
    
    coverage_pct = (tagged_tables / total_tables * 100) if total_tables > 0 else 0
    
    print(f"📊 Tag Coverage Report")
    print(f"   Total Tables: {total_tables}")
    print(f"   Tagged Tables: {tagged_tables}")
    print(f"   Coverage: {coverage_pct:.1f}%")
    
    # Get tag distribution
    tag_dist = spark.sql(f"""
        SELECT tag_name, COUNT(DISTINCT table_name) as table_count
        FROM system.information_schema.table_tags
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}'
        GROUP BY tag_name
        ORDER BY table_count DESC
    """)
    
    print(f"\n   Tag Distribution:")
    for row in tag_dist.collect():
        print(f"      {row['tag_name']}: {row['table_count']} tables")

tag_coverage_report()

# COMMAND ----------

print("✅ Classification tags setup complete")
