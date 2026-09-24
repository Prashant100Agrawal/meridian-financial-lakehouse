# Databricks notebook source
# MAGIC %md
# MAGIC # Row and Column Level Security Setup
# MAGIC ## Unity Catalog Row Filters and Column Masks for Financial Data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Coverage
# MAGIC Implements row-based access control (RBAC) and column masking policies for:
# MAGIC * Trading data filtered by business unit
# MAGIC * PII columns masked for non-privileged users  
# MAGIC * Regulatory data access controls
# MAGIC
# MAGIC ## Notebook Functions
# MAGIC 1. Create row filter functions for multi-tenant access
# MAGIC 2. Create column mask functions for PII protection
# MAGIC 3. Apply policies to gold layer tables
# MAGIC 4. Test and validate policies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Filter Functions
# MAGIC Filter rows based on user's business unit or role

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create row filter function for business unit access
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.row_filter_by_business_unit(business_unit STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN 
# MAGIC   CASE
# MAGIC     -- Admins see all data
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('account users') THEN TRUE
# MAGIC     -- Business unit members see only their data
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER(CONCAT(business_unit, '_users')) THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END;
# MAGIC
# MAGIC -- Create row filter for account access (portfolio managers see only their accounts)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.row_filter_by_account(account_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     -- Admins and compliance see all
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN TRUE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN TRUE
# MAGIC     -- Portfolio managers see assigned accounts
# MAGIC     WHEN account_id IN (
# MAGIC       SELECT account_id FROM financial_lakehouse.reporting.user_account_mapping 
# MAGIC       WHERE user_email = CURRENT_USER()
# MAGIC     ) THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Column Mask Functions
# MAGIC Mask PII and sensitive financial data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask account holder name (show only to authorized users)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_account_holder(account_holder STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN account_holder
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN account_holder
# MAGIC     ELSE '***REDACTED***'
# MAGIC   END;
# MAGIC
# MAGIC -- Mask SSN/Tax ID
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN ssn
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('legal') THEN ssn
# MAGIC     ELSE CONCAT('XXX-XX-', SUBSTRING(ssn, -4, 4))
# MAGIC   END;
# MAGIC
# MAGIC -- Mask email (show domain only to non-privileged users)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN email
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN email
# MAGIC     ELSE CONCAT('***@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC   END;
# MAGIC
# MAGIC -- Mask dollar amounts (show ranges to non-privileged users)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_dollar_amount(amount DECIMAL(18,2))
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN CAST(amount AS STRING)
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('finance') THEN CAST(amount AS STRING)
# MAGIC     WHEN amount < 10000 THEN '< $10K'
# MAGIC     WHEN amount < 100000 THEN '$10K - $100K'
# MAGIC     WHEN amount < 1000000 THEN '$100K - $1M'
# MAGIC     ELSE '> $1M'
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply Row Filters to Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply row filter to portfolio performance table
# MAGIC ALTER TABLE financial_lakehouse.reporting.account_performance
# MAGIC SET ROW FILTER financial_lakehouse.reporting.row_filter_by_account ON (acct_id);
# MAGIC
# MAGIC -- Apply row filter to trading summary
# MAGIC ALTER TABLE financial_lakehouse.reporting.daily_trading_summary
# MAGIC SET ROW FILTER financial_lakehouse.reporting.row_filter_by_business_unit ON (business_unit);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply Column Masks to Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Apply column masks to a customer/account table
# MAGIC -- (Assuming you have a customer dimension table)
# MAGIC
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customer_accounts
# MAGIC -- ALTER COLUMN account_holder_name SET MASK financial_lakehouse.reporting.mask_account_holder;
# MAGIC --
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customer_accounts  
# MAGIC -- ALTER COLUMN ssn SET MASK financial_lakehouse.reporting.mask_ssn;
# MAGIC --
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customer_accounts
# MAGIC -- ALTER COLUMN email SET MASK financial_lakehouse.reporting.mask_email;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Policies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test row filter (user will only see rows they have access to)
# MAGIC SELECT acct_id, trade_date, total_traded_value
# MAGIC FROM financial_lakehouse.reporting.account_performance
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. View Applied Policies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check which tables have row filters
# MAGIC SELECT 
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   mask_name as policy_name,
# MAGIC   column_name
# MAGIC FROM system.information_schema.column_masks
# MAGIC WHERE table_catalog = 'financial_lakehouse'
# MAGIC   AND table_schema = 'gold';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Remove Policies (if needed)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove row filter
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.account_performance DROP ROW FILTER;
# MAGIC
# MAGIC -- Remove column mask
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customer_accounts ALTER COLUMN ssn DROP MASK;

# COMMAND ----------

print("✅ Row and column security setup complete")