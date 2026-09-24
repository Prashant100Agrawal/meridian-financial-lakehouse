# Databricks notebook source
# MAGIC %md
# MAGIC # Logging Utilities
# MAGIC ## Centralized logging for pipelines

# COMMAND ----------

import logging
from datetime import datetime

# COMMAND ----------

class PipelineLogger:
    """Custom logger for data pipelines"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.logger = logging.getLogger(pipeline_name)
        self.logger.setLevel(logging.INFO)
    
    def info(self, message):
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.info(log_msg)
        print(f"{datetime.now()} - INFO - {log_msg}")
    
    def error(self, message):
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.error(log_msg)
        print(f"{datetime.now()} - ERROR - {log_msg}")
    
    def warning(self, message):
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.warning(log_msg)
        print(f"{datetime.now()} - WARNING - {log_msg}")

# COMMAND ----------

# Example usage
logger = PipelineLogger("trading_systems_pipeline")
logger.info("Pipeline started")
logger.info("Processing 1000 records")
logger.info("Pipeline completed successfully")
