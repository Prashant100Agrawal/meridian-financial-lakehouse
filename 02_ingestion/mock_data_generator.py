# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Mock Data Generator
# MAGIC ## Generates realistic mock financial data

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid
import builtins

# Preserve Python's built-in round function
py_round = builtins.round

# COMMAND ----------

# DBTITLE 1,Cell 3
def generate_trading_data(num_records=1000):
    """Generate mock trading transactions"""
    
    instruments = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'JPM', 'BAC', 'GS']
    transaction_types = ['BUY', 'SELL']
    currencies = ['USD', 'EUR', 'GBP']
    
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': str(uuid.uuid4()),
            'account_id': f'ACC{random.randint(1000, 9999)}',
            'instrument_id': random.choice(instruments),
            'transaction_type': random.choice(transaction_types),
            'quantity': py_round(random.uniform(1, 1000), 4),
            'price': py_round(random.uniform(50, 500), 4),
            'transaction_timestamp': datetime.now() - timedelta(minutes=random.randint(0, 60)),
            'currency': random.choice(currencies),
            'trader_id': f'TRD{random.randint(100, 999)}',
            'ingestion_timestamp': datetime.now()
        })
    
    return spark.createDataFrame(data)

# COMMAND ----------

# DBTITLE 1,Cell 4
def generate_market_data(num_records=100):
    """Generate mock market data"""
    
    instruments = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'JPM', 'BAC', 'GS']
    
    data = []
    for instrument in instruments:
        base_price = random.uniform(100, 500)
        for _ in range(num_records // len(instruments)):
            bid = py_round(base_price * random.uniform(0.99, 1.00), 4)
            ask = py_round(base_price * random.uniform(1.00, 1.01), 4)
            data.append({
                'instrument_id': instrument,
                'quote_timestamp': datetime.now() - timedelta(minutes=random.randint(0, 60)),
                'bid_price': bid,
                'ask_price': ask,
                'last_price': py_round((bid + ask) / 2, 4),
                'volume': random.randint(100000, 10000000),
                'market_cap': py_round(random.uniform(1e9, 1e12), 2),
                'ingestion_timestamp': datetime.now()
            })
    
    return spark.createDataFrame(data)

# COMMAND ----------

# DBTITLE 1,Cell 5
def generate_risk_metrics(num_records=50):
    """Generate mock risk metrics"""
    
    data = []
    for i in range(num_records):
        data.append({
            'portfolio_id': f'PF{random.randint(1000, 9999)}',
            'metric_timestamp': datetime.now() - timedelta(hours=random.randint(0, 24)),
            'var_95': py_round(random.uniform(0.01, 0.05), 6),
            'var_99': py_round(random.uniform(0.02, 0.08), 6),
            'expected_shortfall': py_round(random.uniform(0.03, 0.10), 6),
            'beta': py_round(random.uniform(0.5, 1.5), 6),
            'sharpe_ratio': py_round(random.uniform(0.5, 2.5), 6),
            'ingestion_timestamp': datetime.now()
        })
    
    return spark.createDataFrame(data)

# COMMAND ----------

# DBTITLE 1,Cell 6
# Test the generators
print("Testing mock data generators...")

# Generate sample data
trading_df = generate_trading_data(10)
market_df = generate_market_data(10)
risk_df = generate_risk_metrics(10)

print(f"Trading data: {trading_df.count()} records")
print(f"Market data: {market_df.count()} records")
print(f"Risk metrics: {risk_df.count()} records")

# Display samples
print("\n📊 Sample Trading Data:")
display(trading_df.limit(3))

print("\n📊 Sample Market Data:")
display(market_df.limit(3))

print("\n📊 Sample Risk Metrics:")
display(risk_df.limit(3))