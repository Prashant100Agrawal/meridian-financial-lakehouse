# Setup Guide

## Prerequisites
* Databricks workspace
* Unity Catalog enabled
* Serverless compute available

## Step-by-Step Setup

### 1. Create Catalog and Schemas
Run notebook: `00_setup/01_create_catalogs_schemas.sql`

This creates:
* Catalog: `financial_lakehouse`
* Schemas: `raw`, `bronze`, `silver`, `gold`

### 2. Create Volumes
Run notebook: `00_setup/02_create_volumes.sql`

Creates volumes for:
* Raw file landing
* Checkpoint storage

### 3. Generate Mock Data
Run notebook: `02_ingestion/mock_data_generator.py`

Generates sample data for:
* Trading systems
* Market data
* Risk metrics

### 4. Ingest to Raw Layer
Run notebook: `02_ingestion/api_ingest_raw.py`

Simulates API calls and stores data in raw layer.

### 5. Load Bronze Layer
Run notebooks in `03_bronze/`:
* `bronze_trading_systems.py`
* `bronze_external_apis.py`

### 6. Process Silver Layer
Run notebooks in `04_silver/`:
* `silver_cleanse_deduplicate.py`
* `silver_data_quality_checks.py`
* `silver_conform_enrich.py`

### 7. Build Gold Layer
Run notebooks in `05_gold/`:
* `gold_financial_analytics.py`
* `gold_risk_measures.py`
* `gold_regulatory_reporting.py`

### 8. Create Analytics Dashboard
Use SQL queries in `08_analytics/sql_queries/` to build dashboards.

## Scheduling with Jobs

1. Create a Databricks Job
2. Add tasks for each layer:
   * Ingestion (every 15 minutes)
   * Bronze processing
   * Silver processing
   * Gold aggregation
3. Set up dependencies between tasks
4. Enable email notifications

## Monitoring

* Check pipeline run logs: `financial_lakehouse.gold.pipeline_run_log`
* Review data quality metrics: `financial_lakehouse.gold.dq_metrics_log`
* Monitor table sizes and freshness

## Next Steps

* Customize schemas for your data sources
* Add more data quality rules
* Build dashboards for your use cases
* Set up alerting and monitoring
* Implement incremental processing
