# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Basel III Regulatory Reporting
# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Tier: Basel III Capital Adequacy
# MAGIC ## Computes Basel III regulatory capital requirements
# MAGIC ### Frameworks: Basel III, CCAR, IFRS 9

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, max as _max, current_timestamp, lit, when, round as _round

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Risk-Weighted Assets (RWA) Calculation

# COMMAND ----------

# Read from reporting layer for trade-level risk weights
trading_df = spark.table('financial_lakehouse.reporting.daily_trading_summary')
account_df = spark.table('financial_lakehouse.reporting.account_performance')

# Calculate RWA by asset class (Basel III standardized approach)
rwa_df = trading_df.groupBy('symbol', 'side').agg(
    _sum('total_value').alias('exposure'),
    count('symbol').alias('trade_count')
).withColumn(
    'risk_weight',
    when(col('side') == 'BUY', 0.20)  # 20% for equity holdings
    .when(col('side') == 'SELL', 0.10)  # 10% for short positions
    .otherwise(0.50)  # 50% for other
).withColumn(
    'risk_weighted_assets',
    _round(col('exposure') * col('risk_weight'), 2)
)

# Save RWA table
rwa_df.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.risk_weighted_assets')
print(f'RWA table created: {rwa_df.count()} records')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Capital Adequacy Ratios

# COMMAND ----------

total_rwa = rwa_df.agg(_sum('risk_weighted_assets')).collect()[0][0] or 0

# Basel III minimum requirements
tier1_capital = 10000000  # Placeholder: $10M Tier 1 capital
total_capital = 15000000  # Placeholder: $15M total capital

capital_ratios = [
    ('Tier 1 Capital Ratio', tier1_capital / total_rwa if total_rwa > 0 else 0, 0.06, '6%'),
    ('Total Capital Ratio', total_capital / total_rwa if total_rwa > 0 else 0, 0.08, '8%'),
    ('Tier 1 Leverage Ratio', tier1_capital / 20000000, 0.04, '4%'),  # leverage denominator placeholder
]

from pyspark.sql import Row
capital_df = spark.createDataFrame([
    Row(
        metric_name=r[0],
        ratio=round(r[1], 4),
        min_required=r[2],
        min_required_label=r[3],
        compliant=r[1] >= r[2],
        report_date=current_timestamp(),
        total_rwa=total_rwa
    ) for r in capital_ratios
])

capital_df.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.capital_adequacy_ratios')
print(f'Capital adequacy table created: {capital_df.count()} records')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Liquidity Coverage Ratio (LCR)

# COMMAND ----------

# Simplified LCR calculation
lcr_data = [
    ('High Quality Liquid Assets', 5000000, None),
    ('Net Cash Outflows (30d)', 3500000, None),
    ('LCR Ratio', None, 5000000 / 3500000 if 3500000 > 0 else 0),
]

lcr_df = spark.createDataFrame([
    Row(
        component=r[0],
        amount=r[1] if r[1] else 0,
        ratio=r[2] if r[2] else 0,
        report_date=current_timestamp(),
        min_required=1.00 if 'Ratio' in r[0] else None
    ) for r in lcr_data
])

lcr_df.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.liquidity_coverage_ratio')
print(f'LCR table created: {lcr_df.count()} records')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compliance Summary

# COMMAND ----------

print(f'\n=== Basel III Regulatory Report ===')
print(f'  Total RWA: ${total_rwa:,.2f}')
print(f'  Tier 1 Capital Ratio: {capital_ratios[0][1]*100:.2f}% (min: {capital_ratios[0][3]})')
print(f'  Total Capital Ratio: {capital_ratios[1][1]*100:.2f}% (min: {capital_ratios[1][3]})')
print(f'  LCR: {lcr_data[2][2]*100:.2f}% (min: 100%)')
print(f'  Tables created: risk_weighted_assets, capital_adequacy_ratios, liquidity_coverage_ratio')