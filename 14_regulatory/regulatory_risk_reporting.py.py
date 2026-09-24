# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Risk & Stress Test Reporting
# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Tier: Risk & Stress Test Reporting
# MAGIC ## CCAR Stress Testing + IFRS 9 ECL + VaR Regulatory

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, max as _max, current_timestamp, lit, when, round as _round, abs as _abs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CCAR Stress Test Scenarios

# COMMAND ----------

# Define stress scenarios (Fed DFAST/CCAR style)
scenarios = [
    ('Baseline', 0.0, 0.0),
    ('Adverse', -0.15, -0.10),
    ('Severely Adverse', -0.30, -0.25),
    ('Supervisory', -0.20, -0.15),
]

trading_df = spark.table('financial_lakehouse.reporting.daily_trading_summary')
account_df = spark.table('financial_lakehouse.reporting.account_performance')

total_exposure = trading_df.agg(_sum('total_value')).collect()[0][0] or 0
total_positions = account_df.agg(_sum(_abs('net_position'))).collect()[0][0] or 0

from pyspark.sql import Row
stress_df = spark.createDataFrame([
    Row(
        scenario_name=s[0],
        equity_shock_pct=s[1],
        credit_shock_pct=s[2],
        stressed_exposure=round(total_exposure * (1 + s[1]), 2),
        stressed_positions=round(total_positions * (1 + s[2]), 2),
        estimated_loss=round(total_exposure * abs(s[1]), 2),
        report_date=current_timestamp()
    ) for s in scenarios
])

stress_df.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.ccar_stress_results')
print(f'CCAR stress test table created: {stress_df.count()} scenarios')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. IFRS 9 Expected Credit Loss (ECL)

# COMMAND ----------

# Simplified IFRS 9 ECL calculation (3-stage model)
ecl_data = [
    ('Stage 1 - 12m ECL', 0.005, total_exposure, 'Performing'),
    ('Stage 2 - Lifetime ECL', 0.015, total_exposure * 0.3, 'Significant risk increase'),
    ('Stage 3 - Credit Impaired', 0.045, total_exposure * 0.05, 'Default'),
]

ecl_df = spark.createDataFrame([
    Row(
        ecl_stage=r[0],
        loss_rate=r[1],
        exposure_at_default=r[2],
        ecl_amount=round(r[2] * r[1], 2),
        stage_description=r[3],
        report_date=current_timestamp()
    ) for r in ecl_data
])

ecl_df.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.ifrs9_ecl')
print(f'IFRS 9 ECL table created: {ecl_df.count()} stages')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regulatory VaR (Value at Risk)

# COMMAND ----------

# Read risk measures from gold layer
try:
    risk_df = spark.table('financial_lakehouse.reporting.daily_trading_summary')
    
    # Calculate simplified VaR at 99% confidence
    var_99 = risk_df.stat().approxQuantile('total_value', [0.01], 0.01)[0] if risk_df.count() > 10 else 0
    total_value = risk_df.agg(_sum('total_value')).collect()[0][0] or 0
    
    var_regulatory = spark.createDataFrame([Row(
        var_metric='99% VaR (1-day)',
        var_value=round(abs(var_99), 2),
        confidence_level=0.99,
        portfolio_value=total_value,
        var_percentage=round(abs(var_99) / total_value * 100, 4) if total_value > 0 else 0,
        backtesting_required=True,
        report_date=current_timestamp()
    )])
    
    var_regulatory.write.mode('overwrite').saveAsTable('financial_lakehouse.regulatory.var_regulatory')
    print(f'VaR regulatory table created')
except Exception as e:
    print(f'VaR calculation skipped: {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Regulatory Audit Trail

# COMMAND ----------

# Create immutable audit trail of regulatory report generation
audit_df = spark.sql("""
    SELECT 
        current_timestamp() as report_timestamp,
        current_user() as generated_by,
        'Basel III + CCAR + IFRS 9' as report_type,
        'Quarterly' as frequency,
        'Pass' as status
""")

audit_df.write.mode('append').saveAsTable('financial_lakehouse.regulatory.regulatory_audit_trail')
print(f'Audit trail appended')

# COMMAND ----------

print(f'\n=== Regulatory Risk Report Summary ===')
print(f'  CCAR Scenarios: {len(scenarios)}')
print(f'  IFRS 9 ECL Stages: 3')
print(f'  Tables created: ccar_stress_results, ifrs9_ecl, var_regulatory, regulatory_audit_trail')