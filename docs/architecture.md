# Meridian Financial Lakehouse - Architecture

## Overview
This project implements a complete financial data lakehouse on Databricks, following the medallion architecture pattern (Bronze, Silver, Gold).

## Architecture Components

### 1. Data Sources
* **Trading Systems**: Market and trade execution data
* **Enterprise Systems**: ERP and financial systems
* **Custody Platforms**: Positions and settlements
* **Vendor Files**: External data feeds
* **External APIs**: Market data and regulatory feeds

### 2. Ingestion Layer (Raw)
* API ingestion every 15 minutes
* Auto Loader for streaming ingestion
* Raw data landing in Unity Catalog volumes
* Mock data generation for testing

### 3. Bronze Layer
* Immutable raw data
* Minimal transformations
* Full audit trail
* Schema evolution support

### 4. Silver Layer
* Cleansed and deduplicated data
* Data quality checks
* Standardized schemas
* Business rules applied

### 5. Gold Layer
* Business-ready aggregates
* Domain-specific data products:
  - Financial Analytics
  - Portfolio Performance
  - Risk Measures
  - Regulatory Reporting

### 6. Consumption Layer
* SQL Analytics
* BI Dashboards
* Interactive reporting

## Unity Catalog Structure

```
financial_lakehouse (catalog)
├── raw (schema)
│   ├── landing_files (volume)
│   └── checkpoints (volume)
├── bronze (schema)
│   ├── trading_systems
│   ├── market_data
│   └── risk_metrics
├── silver (schema)
│   ├── trading_systems_clean
│   └── trading_systems_conformed
└── gold (schema)
    ├── daily_trading_summary
    ├── account_performance
    ├── risk_summary
    └── regulatory_large_trades
```

## Data Governance
* Row-level security
* Column-level masking
* Audit logging
* Data lineage tracking

## Orchestration
* Databricks Jobs for scheduling
* Workflow automation
* Monitoring and alerting
