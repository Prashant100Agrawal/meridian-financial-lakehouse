# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Quality Checks
# MAGIC ## Validate data quality rules

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

silver_table = "financial_lakehouse.silver.trading_systems_clean"
df = spark.table(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Checks

# COMMAND ----------

# Check 1: Price should be positive
check_positive_price = df.filter(col("price") <= 0).count()
print(f"Records with non-positive price: {check_positive_price}")

# Check 2: Quantity should be positive
check_positive_qty = df.filter(col("quantity") <= 0).count()
print(f"Records with non-positive quantity: {check_positive_qty}")

# Check 3: Valid currency codes
valid_currencies = ['USD', 'EUR', 'GBP', 'JPY']
check_currency = df.filter(~col("currency").isin(valid_currencies)).count()
print(f"Records with invalid currency: {check_currency}")

# Check 4: Transaction timestamp in valid range
check_future_dates = df.filter(col("transaction_timestamp") > current_timestamp()).count()
print(f"Records with future timestamps: {check_future_dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_records = df.count()
failed_checks = check_positive_price + check_positive_qty + check_currency + check_future_dates

print(f"\n{'='*50}")
print(f"Total records: {total_records}")
print(f"Failed quality checks: {failed_checks}")
print(f"Pass rate: {((total_records - failed_checks) / total_records * 100):.2f}%")
print(f"{'='*50}")
