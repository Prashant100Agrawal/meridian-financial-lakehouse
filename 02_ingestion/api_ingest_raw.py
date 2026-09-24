# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # API Ingestion to Raw Layer
# MAGIC ## Calls API every 15 minutes and stores data in raw layer

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./mock_data_generator

# COMMAND ----------

# Load configuration
with open('../01_config/api_config.json', 'r') as f:
    config = json.load(f)

# COMMAND ----------

# DBTITLE 1,Cell 5
def ingest_to_raw(data_source, df):
    """Write data to raw layer"""
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_path = f"/Volumes/financial_lakehouse/raw/landing_files/{data_source}/{timestamp_str}"
    
    # Add ingestion metadata
    df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                         .withColumn("_source_system", lit(data_source))
    
    # Write to raw layer as parquet
    df_with_metadata.write.mode("append").parquet(raw_path)
    
    print(f"✅ Ingested {df.count()} records to {raw_path}")
    return raw_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Trading Systems Data

# COMMAND ----------

# Generate mock trading data (in production, this would be an API call)
trading_df = generate_trading_data(num_records=500)
ingest_to_raw("trading_systems", trading_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Market Data

# COMMAND ----------

market_df = generate_market_data(num_records=100)
ingest_to_raw("market_data", market_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Risk Metrics

# COMMAND ----------

risk_df = generate_risk_metrics(num_records=50)
ingest_to_raw("risk_metrics", risk_df)

# COMMAND ----------

# DBTITLE 1,Enhanced API Client with Retry Logic
# Enhanced API ingestion with pagination, retry, and rate limiting
import time
from typing import List, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class APIIngestionClient:
    """Enhanced API client with retry logic and pagination"""
    
    def __init__(self, base_url: str, max_retries: int = 3, backoff_factor: float = 0.5):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def fetch_paginated_data(self, endpoint: str, page_size: int = 100, max_pages: int = None) -> List[Dict]:
        """Fetch data with pagination support"""
        all_data = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
                
            try:
                # In production, this would call a real API
                # For demo, we'll simulate pagination
                print(f"📡 Fetching page {page}...")
                
                # Simulate API call delay
                time.sleep(0.1)
                
                # Mock API response (in production, uncomment below)
                # response = self.session.get(
                #     f"{self.base_url}/{endpoint}",
                #     params={"page": page, "page_size": page_size},
                #     timeout=30
                # )
                # response.raise_for_status()
                # data = response.json()
                
                # For demo: simulate no more data after first page
                if page > 1:
                    break
                    
                print(f"✓ Fetched page {page}")
                page += 1
                
            except Exception as e:
                print(f"⚠ Error fetching page {page}: {e}")
                break
        
        return all_data

print("✓ Enhanced API Ingestion Client loaded")

# COMMAND ----------

# DBTITLE 1,Rate Limiting & Error Handling
# API Rate Limiting and Error Handling
import time
from functools import wraps

def rate_limit(calls_per_second=10):
    """Decorator to enforce rate limiting"""
    min_interval = 1.0 / calls_per_second
    last_called = [0.0]
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            
            ret = func(*args, **kwargs)
            last_called[0] = time.time()
            return ret
        return wrapper
    return decorator

def retry_with_backoff(retries=3, backoff_factor=2):
    """Decorator for exponential backoff retry"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        print(f"❌ Failed after {retries} attempts: {e}")
                        raise
                    wait_time = backoff_factor ** attempt
                    print(f"⚠ Attempt {attempt + 1} failed, retrying in {wait_time}s...")
                    time.sleep(wait_time)
        return wrapper
    return decorator

print("✓ Rate limiting and retry utilities loaded")