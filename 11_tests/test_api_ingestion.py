# Databricks notebook source
# MAGIC %md
# MAGIC # API Ingestion Tests
# MAGIC ## Tests for data ingestion logic

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

# COMMAND ----------

class TestAPIIngestion(unittest.TestCase):
    """Test API ingestion functions"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
    
    def test_raw_data_exists(self):
        """Test that raw data was ingested"""
        # Check if raw path has data
        try:
            df = self.spark.read.parquet("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")
            record_count = df.count()
            self.assertGreater(record_count, 0, "No data in raw layer")
        except Exception as e:
            self.fail(f"Failed to read raw data: {str(e)}")
    
    def test_ingestion_metadata(self):
        """Test that ingestion metadata columns exist"""
        df = self.spark.read.parquet("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")
        
        self.assertIn("_ingestion_timestamp", df.columns, "Missing _ingestion_timestamp")
        self.assertIn("_source_system", df.columns, "Missing _source_system")

# COMMAND ----------

# Run tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestAPIIngestion)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)