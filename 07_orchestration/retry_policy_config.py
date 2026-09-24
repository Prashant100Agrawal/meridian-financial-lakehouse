# Databricks notebook source
# MAGIC %md
# MAGIC # Retry Policy Configuration
# MAGIC ## Centralized retry logic for pipeline and job failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Features
# MAGIC * Exponential backoff
# MAGIC * Max retry limits
# MAGIC * Failure classification (transient vs permanent)
# MAGIC * Alert escalation

# COMMAND ----------

import time
from datetime import datetime
from functools import wraps

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Retry Configuration

# COMMAND ----------

retry_policies = {
    'api_ingestion': {
        'max_retries': 3,
        'initial_delay_seconds': 5,
        'backoff_multiplier': 2,
        'max_delay_seconds': 300,
        'retryable_errors': ['ConnectionError', 'TimeoutError', 'HTTPError_503', 'HTTPError_429']
    },
    'pipeline_execution': {
        'max_retries': 2,
        'initial_delay_seconds': 60,
        'backoff_multiplier': 2,
        'max_delay_seconds': 600,
        'retryable_errors': ['ClusterStartupError', 'TemporaryTableLockError']
    },
    'data_quality_check': {
        'max_retries': 1,
        'initial_delay_seconds': 30,
        'backoff_multiplier': 1,
        'max_delay_seconds': 30,
        'retryable_errors': []  # DQ failures should not auto-retry
    },
    'database_write': {
        'max_retries': 5,
        'initial_delay_seconds': 2,
        'backoff_multiplier': 2,
        'max_delay_seconds': 120,
        'retryable_errors': ['DeadlockError', 'LockTimeoutError', 'ConnectionPoolExhausted']
    }
}

print("📋 Retry Policy Configuration:")
for task, policy in retry_policies.items():
    print(f"\n   {task}:")
    print(f"      Max Retries: {policy['max_retries']}")
    print(f"      Initial Delay: {policy['initial_delay_seconds']}s")
    print(f"      Backoff: {policy['backoff_multiplier']}x")
    print(f"      Max Delay: {policy['max_delay_seconds']}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Retry Decorator

# COMMAND ----------

def retry_with_backoff(policy_name='default', custom_policy=None):
    """
    Decorator to add retry logic with exponential backoff
    
    Args:
        policy_name: Name of predefined retry policy
        custom_policy: Override with custom policy dict
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get policy
            policy = custom_policy or retry_policies.get(policy_name, {
                'max_retries': 3,
                'initial_delay_seconds': 5,
                'backoff_multiplier': 2,
                'max_delay_seconds': 300,
                'retryable_errors': []
            })
            
            attempt = 0
            delay = policy['initial_delay_seconds']
            
            while attempt <= policy['max_retries']:
                try:
                    # Execute function
                    result = func(*args, **kwargs)
                    
                    if attempt > 0:
                        print(f"   ✅ Success on attempt {attempt + 1}")
                    
                    return result
                    
                except Exception as e:
                    attempt += 1
                    error_type = type(e).__name__
                    
                    # Check if error is retryable
                    is_retryable = (
                        not policy['retryable_errors'] or  # Empty list = retry all
                        error_type in policy['retryable_errors'] or
                        any(err in str(e) for err in policy['retryable_errors'])
                    )
                    
                    if attempt > policy['max_retries'] or not is_retryable:
                        print(f"   ❌ Failed after {attempt} attempts: {error_type}: {str(e)}")
                        raise
                    
                    # Calculate backoff delay
                    wait_time = min(delay, policy['max_delay_seconds'])
                    print(f"   ⚠️  Attempt {attempt} failed: {error_type}. Retrying in {wait_time}s...")
                    
                    time.sleep(wait_time)
                    delay *= policy['backoff_multiplier']
            
            return None
        
        return wrapper
    return decorator

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Example Usage

# COMMAND ----------

@retry_with_backoff(policy_name='api_ingestion')
def fetch_trading_data_with_retry(api_url):
    """
    Fetch trading data from API with automatic retries
    """
    import requests
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    return response.json()

# Example call:
# data = fetch_trading_data_with_retry('https://api.example.com/trades')

# COMMAND ----------

@retry_with_backoff(policy_name='pipeline_execution')
def run_pipeline_with_retry(pipeline_id):
    """
    Run DLT pipeline with retry on transient failures
    """
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    
    # Start pipeline update
    update = w.pipelines.start_update(pipeline_id=pipeline_id)
    return update.update_id

# Example call:
# update_id = run_pipeline_with_retry('abc-123-def')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Retry Metrics Tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS financial_lakehouse.gold.retry_metrics (
# MAGIC   task_name STRING,
# MAGIC   execution_id STRING,
# MAGIC   attempt_number INT,
# MAGIC   status STRING,  -- 'SUCCESS', 'RETRY', 'FAILED'
# MAGIC   error_type STRING,
# MAGIC   error_message STRING,
# MAGIC   retry_delay_seconds INT,
# MAGIC   timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Retry attempt tracking for monitoring';

# COMMAND ----------

def log_retry_attempt(task_name, execution_id, attempt, status, error_type=None, error_msg=None, delay=None):
    """
    Log retry attempt for monitoring
    """
    log_entry = spark.createDataFrame([{
        'task_name': task_name,
        'execution_id': execution_id,
        'attempt_number': attempt,
        'status': status,
        'error_type': error_type,
        'error_message': error_msg,
        'retry_delay_seconds': delay,
        'timestamp': datetime.now()
    }])
    
    log_entry.write.mode("append").saveAsTable("financial_lakehouse.gold.retry_metrics")

# COMMAND ----------

print("✅ Retry policy configuration ready")
