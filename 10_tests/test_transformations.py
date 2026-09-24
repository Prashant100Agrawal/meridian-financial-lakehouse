# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation Tests
# MAGIC ## Unit tests for data transformations

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

class TestTransformations(unittest.TestCase):
    """Test data transformation logic"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
    
    def test_total_value_calculation(self):
        """Test that total_value = qty * unit_price"""
        df = self.spark.table("financial_lakehouse.silver.trading_systems_conformed")
        
        df_calc = df.withColumn("expected_value", col("qty") * col("unit_price"))
        df_calc = df_calc.withColumn("value_diff", abs(col("total_value") - col("expected_value")))
        
        # Allow small floating point differences
        incorrect_calcs = df_calc.filter("value_diff > 0.01").count()
        self.assertEqual(incorrect_calcs, 0, "Found incorrect total_value calculations")
    
    def test_conformed_column_names(self):
        """Test that conformed table has expected columns"""
        df = self.spark.table("financial_lakehouse.silver.trading_systems_conformed")
        
        expected_cols = ['txn_id', 'acct_id', 'symbol', 'side', 'qty', 'unit_price', 'total_value']
        actual_cols = df.columns
        
        for col_name in expected_cols:
            self.assertIn(col_name, actual_cols, f"Missing column: {col_name}")

# COMMAND ----------

# Run tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestTransformations)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)
