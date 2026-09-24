# Databricks notebook source
# MAGIC %md
# MAGIC # Logging Utilities
# MAGIC ## Centralized structured logging for Meridian Financial Lakehouse pipelines
# MAGIC
# MAGIC Features:
# MAGIC * **Dual output** - console (stdout) + Delta table (persistent)
# MAGIC * **Structured logs** - queryable via SQL for monitoring & alerting
# MAGIC * **Run tracking** - link logs to job/pipeline run IDs
# MAGIC * **Performance metrics** - records processed, duration tracking

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import current_user

# COMMAND ----------

# Target table for persistent pipeline logs
PIPELINE_LOG_TABLE = "financial_lakehouse.operational.pipeline_logs"

# COMMAND ----------

class PipelineLogger:
    """Custom logger that writes to both console and Delta table"""
    
    def __init__(self, pipeline_name, run_id=None):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.logger = logging.getLogger(pipeline_name)
        self.logger.setLevel(logging.INFO)
        self._start_times = {}
        
    def _write_to_delta(self, level, message, step_name=None, 
                        records_processed=None, duration_seconds=None, 
                        error_details=None):
        """Write a single log record to the Delta table"""
        try:
            log_row = Row(
                log_timestamp=datetime.now(),
                pipeline_name=self.pipeline_name,
                log_level=level,
                message=message,
                step_name=step_name,
                records_processed=records_processed,
                duration_seconds=duration_seconds,
                error_details=error_details,
                run_id=self.run_id,
                created_by=spark.sql("SELECT current_user() as u").collect()[0][0]
            )
            df = spark.createDataFrame([log_row], schema="""
                log_timestamp TIMESTAMP,
                pipeline_name STRING,
                log_level STRING,
                message STRING,
                step_name STRING,
                records_processed BIGINT,
                duration_seconds DOUBLE,
                error_details STRING,
                run_id STRING,
                created_by STRING
            """)
            df.write.mode("append").saveAsTable(PIPELINE_LOG_TABLE)
        except Exception as e:
            # Don't let logging failures break the pipeline
            print(f"⚠️ Failed to write log to Delta: {e}")
    
    def info(self, message, step_name=None, records_processed=None, 
             duration_seconds=None):
        """Log INFO message to console + Delta"""
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.info(log_msg)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} - INFO  - {log_msg}")
        self._write_to_delta("INFO", message, step_name, 
                            records_processed, duration_seconds)
    
    def warning(self, message, step_name=None, error_details=None):
        """Log WARNING message to console + Delta"""
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.warning(log_msg)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} - WARN  - {log_msg}")
        self._write_to_delta("WARNING", message, step_name, 
                            error_details=error_details)
    
    def error(self, message, step_name=None, error_details=None):
        """Log ERROR message to console + Delta"""
        log_msg = f"[{self.pipeline_name}] {message}"
        self.logger.error(log_msg)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{ts} - ERROR - {log_msg}")
        self._write_to_delta("ERROR", message, step_name, 
                            error_details=error_details)
    
    def step_start(self, step_name):
        """Mark the beginning of a pipeline step (for duration tracking)"""
        self._start_times[step_name] = datetime.now()
        self.info(f"Step started: {step_name}", step_name=step_name)
    
    def step_end(self, step_name, records_processed=None):
        """Mark the end of a pipeline step with duration"""
        start = self._start_times.get(step_name)
        duration = (datetime.now() - start).total_seconds() if start else None
        self.info(
            f"Step completed: {step_name}", 
            step_name=step_name,
            records_processed=records_processed,
            duration_seconds=duration
        )
        if step_name in self._start_times:
            del self._start_times[step_name]

# COMMAND ----------

# Example usage
logger = PipelineLogger("trading_systems_pipeline", run_id="test_001")
logger.info("Pipeline started")
logger.step_start("bronze_ingestion")
logger.step_end("bronze_ingestion", records_processed=15500)
logger.info("Pipeline completed successfully")
