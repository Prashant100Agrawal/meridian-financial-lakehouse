# Databricks notebook source
# MAGIC %md
# MAGIC # Error Handling Utilities
# MAGIC ## Exception handling and retry logic

# COMMAND ----------

import time
from functools import wraps

# COMMAND ----------

def retry_on_failure(max_retries=3, delay_seconds=5):
    """Decorator to retry function on failure"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    print(f"Attempt {attempt + 1} failed: {str(e)}")
                    print(f"Retrying in {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
        return wrapper
    return decorator

# COMMAND ----------

class PipelineException(Exception):
    """Custom exception for pipeline errors"""
    pass

# COMMAND ----------

# Example usage
@retry_on_failure(max_retries=3, delay_seconds=2)
def read_data_with_retry(path):
    """Read data with automatic retry"""
    return spark.read.parquet(path)
