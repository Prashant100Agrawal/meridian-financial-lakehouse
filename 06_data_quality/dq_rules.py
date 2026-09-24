# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Rules
# MAGIC ## Define and enforce data quality rules

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

class DataQualityRules:
    """Data quality rules for financial data"""
    
    @staticmethod
    def check_completeness(df, required_columns):
        """Check if required columns have no nulls"""
        results = {}
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            total_count = df.count()
            results[col_name] = {
                'null_count': null_count,
                'completeness_pct': ((total_count - null_count) / total_count * 100) if total_count > 0 else 0
            }
        return results
    
    @staticmethod
    def check_validity(df, column, valid_values):
        """Check if column values are within valid set"""
        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        total_count = df.count()
        return {
            'invalid_count': invalid_count,
            'validity_pct': ((total_count - invalid_count) / total_count * 100) if total_count > 0 else 0
        }
    
    @staticmethod
    def check_range(df, column, min_val, max_val):
        """Check if numeric values are within expected range"""
        out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
        total_count = df.count()
        return {
            'out_of_range_count': out_of_range,
            'in_range_pct': ((total_count - out_of_range) / total_count * 100) if total_count > 0 else 0
        }

# COMMAND ----------

# Example usage
df = spark.table("financial_lakehouse.silver.trading_systems_conformed")

print("Data Quality Check Results:")
print("="*50)

# Completeness check
completeness = DataQualityRules.check_completeness(df, ['txn_id', 'acct_id', 'symbol'])
print(f"Completeness: {completeness}")

# Validity check
validity = DataQualityRules.check_validity(df, 'side', ['BUY', 'SELL'])
print(f"Validity: {validity}")

# Range check
range_check = DataQualityRules.check_range(df, 'unit_price', 0, 10000)
print(f"Range: {range_check}")
