-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Unity Catalog Structure
-- MAGIC ## This notebook creates the catalog and schemas for the Financial Lakehouse

-- COMMAND ----------

-- Create the main catalog
CREATE CATALOG IF NOT EXISTS financial_lakehouse;

-- COMMAND ----------

-- Use the catalog
USE CATALOG financial_lakehouse;

-- COMMAND ----------

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS raw COMMENT 'Raw landing zone for API data';
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Bronze layer - raw immutable data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Silver layer - cleansed and conformed data';
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Gold layer - business-ready aggregates';

-- COMMAND ----------

-- Verify schemas
SHOW SCHEMAS IN financial_lakehouse;

-- COMMAND ----------

-- Set permissions (example - adjust as needed)
-- GRANT USAGE, CREATE ON CATALOG financial_lakehouse TO `account_users`;
-- GRANT ALL PRIVILEGES ON SCHEMA financial_lakehouse.raw TO `account_users`;