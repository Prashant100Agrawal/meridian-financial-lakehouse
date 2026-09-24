# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow Runner
# MAGIC ## Orchestrate the entire pipeline

# COMMAND ----------

import time
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../09_utilities/logging_utils

# COMMAND ----------

logger = PipelineLogger("meridian_financial_lakehouse")

# COMMAND ----------

def run_pipeline():
    """Execute the complete data pipeline"""
    
    logger.info("="*60)
    logger.info("Starting Meridian Financial Lakehouse Pipeline")
    logger.info(f"Execution time: {datetime.now()}")
    logger.info("="*60)
    
    try:
        # Step 1: Ingestion
        logger.info("Step 1/5: Running ingestion...")
        dbutils.notebook.run("../02_ingestion/api_ingest_raw", timeout_seconds=600)
        logger.info("✅ Ingestion completed")
        
        # Step 2: Bronze layer
        logger.info("Step 2/5: Processing bronze layer...")
        dbutils.notebook.run("../03_bronze/bronze_trading_systems", timeout_seconds=600)
        dbutils.notebook.run("../03_bronze/bronze_external_apis", timeout_seconds=600)
        logger.info("✅ Bronze layer completed")
        
        # Step 3: Silver layer
        logger.info("Step 3/5: Processing silver layer...")
        dbutils.notebook.run("../04_silver/silver_cleanse_deduplicate", timeout_seconds=600)
        dbutils.notebook.run("../04_silver/silver_data_quality_checks", timeout_seconds=600)
        dbutils.notebook.run("../04_silver/silver_conform_enrich", timeout_seconds=600)
        logger.info("✅ Silver layer completed")
        
        # Step 4: Gold layer
        logger.info("Step 4/5: Processing gold layer...")
        dbutils.notebook.run("../05_gold/gold_financial_analytics", timeout_seconds=600)
        dbutils.notebook.run("../05_gold/gold_risk_measures", timeout_seconds=600)
        dbutils.notebook.run("../05_gold/gold_regulatory_reporting", timeout_seconds=600)
        logger.info("✅ Gold layer completed")
        
        # Step 5: Data quality monitoring
        logger.info("Step 5/5: Running data quality checks...")
        dbutils.notebook.run("../06_data_quality/dq_monitoring", timeout_seconds=300)
        logger.info("✅ Data quality checks completed")
        
        logger.info("="*60)
        logger.info("Pipeline completed successfully!")
        logger.info("="*60)
        
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

# COMMAND ----------

# Execute pipeline
result = run_pipeline()
dbutils.notebook.exit(result)
