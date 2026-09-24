-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Unity Catalog Volumes
-- MAGIC ## Volumes for storing raw files and checkpoints

-- COMMAND ----------

USE CATALOG financial_lakehouse;

-- COMMAND ----------

-- Create volume for raw files
CREATE VOLUME IF NOT EXISTS raw.landing_files;

-- COMMAND ----------

-- Create volume for checkpoints
CREATE VOLUME IF NOT EXISTS raw.checkpoints;

-- COMMAND ----------

-- Verify volumes
SHOW VOLUMES IN raw;