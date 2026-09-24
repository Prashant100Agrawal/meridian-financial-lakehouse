# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests
# MAGIC ## Unit tests for data quality checks

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

# COMMAND ----------

class TestDataQuality(unittest.TestCase):
    """Test data quality functions"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
    
    def test_trading_data_completeness(self):
        """Test that trading data has no null critical fields"""
        df = self.spark.table("financial_lakehouse.standardized.trading_systems_conformed")
        
        null_txn_ids = df.filter("txn_id IS NULL").count()
        null_acct_ids = df.filter("acct_id IS NULL").count()
        null_symbols = df.filter("symbol IS NULL").count()
        
        self.assertEqual(null_txn_ids, 0, "Found null transaction IDs")
        self.assertEqual(null_acct_ids, 0, "Found null account IDs")
        self.assertEqual(null_symbols, 0, "Found null symbols")
    
    def test_price_validity(self):
        """Test that prices are positive"""
        df = self.spark.table("financial_lakehouse.standardized.trading_systems_conformed")
        
        negative_prices = df.filter("unit_price <= 0").count()
        self.assertEqual(negative_prices, 0, "Found negative or zero prices")
    
    def test_side_values(self):
        """Test that side values are valid"""
        df = self.spark.table("financial_lakehouse.standardized.trading_systems_conformed")
        
        invalid_sides = df.filter("side NOT IN ('BUY', 'SELL')").count()
        self.assertEqual(invalid_sides, 0, "Found invalid side values")

# COMMAND ----------

# Run tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQuality)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

print(f"\n{'='*50}")
print(f"Tests run: {result.testsRun}")
print(f"Failures: {len(result.failures)}")
print(f"Errors: {len(result.errors)}")
print(f"{'='*50}")
