# Databricks notebook source
# MAGIC %md
# MAGIC # PII Masking Policies
# MAGIC ## Dynamic data masking for personally identifiable information
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Coverage
# MAGIC * Email addresses
# MAGIC * Phone numbers
# MAGIC * Social Security Numbers / Tax IDs
# MAGIC * Credit card numbers
# MAGIC * Physical addresses
# MAGIC * Account holder names

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create PII Detection Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function to detect if column contains PII
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.is_pii_column(column_name STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   column_name RLIKE '(ssn|tax_id|email|phone|address|card_number|account_holder|customer_name)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Email Masking Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full email masking
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_email_full(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN email IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') OR IS_ACCOUNT_GROUP_MEMBER('compliance') THEN email
# MAGIC     ELSE CONCAT(SUBSTRING(email, 1, 2), '***@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC   END;
# MAGIC
# MAGIC -- Partial email masking (show first char + domain)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_email_partial(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN email IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN email
# MAGIC     ELSE CONCAT(SUBSTRING(email, 1, 1), '***@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Phone Number Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask phone numbers (show last 4 digits)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN phone IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') OR IS_ACCOUNT_GROUP_MEMBER('compliance') THEN phone
# MAGIC     ELSE CONCAT('XXX-XXX-', SUBSTRING(phone, -4, 4))
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SSN / Tax ID Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask SSN (show last 4 digits)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_ssn_last4(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN ssn IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') OR IS_ACCOUNT_GROUP_MEMBER('legal') THEN ssn
# MAGIC     ELSE CONCAT('XXX-XX-', SUBSTRING(ssn, -4, 4))
# MAGIC   END;
# MAGIC
# MAGIC -- Full SSN masking
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_ssn_full(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN ssn IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('compliance') THEN ssn
# MAGIC     ELSE '***-**-****'
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Credit Card Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask credit card (show last 4 digits)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_credit_card(card STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN card IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') OR IS_ACCOUNT_GROUP_MEMBER('finance') THEN card
# MAGIC     ELSE CONCAT('****-****-****-', SUBSTRING(card, -4, 4))
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Address Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask street address (show only city/state)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_address(address STRING, city STRING, state STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN address IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') OR IS_ACCOUNT_GROUP_MEMBER('compliance') THEN address
# MAGIC     ELSE CONCAT(city, ', ', state)
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Name Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask account holder name (show initials)
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_name_initials(full_name STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN full_name IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') OR IS_ACCOUNT_GROUP_MEMBER('compliance') THEN full_name
# MAGIC     ELSE CONCAT(
# MAGIC       SUBSTRING(full_name, 1, 1), 
# MAGIC       '. ',
# MAGIC       SUBSTRING(SUBSTRING_INDEX(full_name, ' ', -1), 1, 1),
# MAGIC       '.'
# MAGIC     )
# MAGIC   END;
# MAGIC
# MAGIC -- Full name masking
# MAGIC CREATE OR REPLACE FUNCTION financial_lakehouse.reporting.mask_name_full(full_name STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN full_name IS NULL THEN NULL
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN full_name
# MAGIC     ELSE '[REDACTED]'
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply Policies to Tables (Template)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Apply to a customer table
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customers
# MAGIC -- ALTER COLUMN email SET MASK financial_lakehouse.reporting.mask_email_full;
# MAGIC --
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customers
# MAGIC -- ALTER COLUMN phone SET MASK financial_lakehouse.reporting.mask_phone;
# MAGIC --
# MAGIC -- ALTER TABLE financial_lakehouse.reporting.customers
# MAGIC -- ALTER COLUMN ssn SET MASK financial_lakehouse.reporting.mask_ssn_last4;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Audit PII Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all columns that might contain PII
# MAGIC SELECT 
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   data_type
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE table_catalog = 'financial_lakehouse'
# MAGIC   AND (
# MAGIC     LOWER(column_name) RLIKE '(email|phone|ssn|tax.*id|card|address|customer.*name|account.*holder)'
# MAGIC     OR LOWER(comment) RLIKE '(pii|personal|sensitive)'
# MAGIC   )
# MAGIC ORDER BY table_name, column_name;

# COMMAND ----------

print("✅ PII masking policies created")
