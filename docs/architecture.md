# Meridian Financial Lakehouse - Architecture

## Overview
This project implements a comprehensive financial data lakehouse on Databricks, following a **business-centric domain-oriented architecture** that aligns with the organization's data management strategy. The architecture supports enterprise-grade financial analytics, risk management, and regulatory compliance.

## Architecture Pattern

**Domain-Oriented Lakehouse** with business-aligned schema layers:
* **Raw** → **Operational** → **Standardized** → **Reporting** → **Regulatory**

This approach replaces the traditional Bronze/Silver/Gold medallion pattern with schemas that directly reflect business functions and stakeholder needs.

## Architecture Components

### 1. Data Sources (Implemented)
* ✅ **Trading Systems**: Market and trade execution data
* ✅ **External APIs**: Market data and regulatory feeds (paginated ingestion)
* ⚠️ **Enterprise Systems**: ERP and financial data (not fully connected)
* ❌ **Custody Platforms**: Positions and settlements (not implemented)
* ❌ **Vendor Files**: Structured data extracts (not fully implemented)

### 2. Ingestion Patterns (Partial)
* ✅ **API Ingestion**: Paginated REST API with retry logic
* ✅ **AWS MSK/Kafka**: Real-time streaming via Auto Loader
* ✅ **Databricks Auto Loader**: Schema inference & evolution
* ❌ **AWS DMS**: Incremental database changes (not implemented)
* ❌ **AWS Glue + S3**: Scheduled batch processing (not implemented)
* ❌ **S3 + Lambda**: Event-driven uploads (not implemented)
* ❌ **Controlled Uploads**: Adjustments and recon files (not implemented)

### 3. LAKEHOUSE 1: Enterprise Data Hub (Operational & Standardized)

#### Bronze → **Operational Schema**
* Immutable raw ingestion landing zone
* Minimal transformations
* Full audit trail with CDC tracking
* Schema evolution support
* **Tables**: `trading_systems`, `market_data`, `risk_metrics`, `pipeline_logs`, `dlt_event_logs`

#### Silver → **Standardized Schema**
* Cleansed and deduplicated data
* Data quality checks with Great Expectations integration
* Reference data joins (currency, counterparty)
* Business rules and conformance applied
* **Tables**: `trading_systems_clean`, `trading_systems_conformed`, `risk_metrics_clean`

### 4. Domain Data Products (Lakehouses 2 & 3)

Implemented as **schemas within a single catalog** (architectural difference from diagram):

#### LAKEHOUSE 2: Financial Analytics (Reporting Schema)
* Business-ready aggregates for Portfolio Performance
* **Tables**:
  - `daily_trading_summary`
  - `account_performance`
  - `risk_summary`
  - `portfolio_metrics`
  - `audit_log_summary`
  - `alert_views` (5 monitoring views)
  - `unified_log_monitoring` (consolidated view)

#### LAKEHOUSE 3: Risk & Compliance (Regulatory Schema)
* Regulatory reporting and audit-ready data
* **Tables**:
  - `regulatory_large_trades`
  - `risk_weighted_assets`
  - `capital_adequacy_ratios`
  - `liquidity_coverage_ratio`
  - `ccar_stress_results`
  - `ifrs9_ecl`
  - `var_regulatory`
  - `regulatory_audit_trail`

### 5. Consumption Layer (Limited)
* ✅ **SQL Analytics**: Serverless SQL Warehouse for queries
* ⚠️ **BI Dashboards**: 1 monitoring dashboard (limited business user dashboards)
* ❌ **Interactive Reporting**: Not fully built out for Portfolio Teams, C-Suite, Audit Stakeholders

## Unity Catalog Structure (Current State)

### Production Catalog
```
financial_lakehouse (catalog)
├── raw (schema)
│   ├── landing_files (volume)
│   ├── checkpoints (volume)
│   ├── monitoring_assets (volume)
│   ├── trading_systems_config (table)
│   └── daily_trading_summary (table)
├── operational (schema) [Bronze equivalent]
│   ├── trading_systems
│   ├── market_data
│   ├── risk_metrics
│   ├── pipeline_logs (structured logging)
│   └── dlt_event_logs (DLT event export)
├── standardized (schema) [Silver equivalent]
│   ├── trading_systems_clean
│   ├── trading_systems_conformed
│   └── risk_metrics_clean
├── reporting (schema) [Gold equivalent - Financial Analytics]
│   ├── daily_trading_summary
│   ├── account_performance
│   ├── risk_summary
│   ├── portfolio_metrics
│   ├── audit_log_summary
│   ├── unified_log_monitoring (view)
│   └── 5 alert views
└── regulatory (schema) [Gold equivalent - Risk & Compliance]
    ├── regulatory_large_trades
    ├── risk_weighted_assets
    ├── capital_adequacy_ratios
    ├── liquidity_coverage_ratio
    ├── ccar_stress_results
    ├── ifrs9_ecl
    ├── var_regulatory
    └── regulatory_audit_trail
```

### Staging Catalog
```
financial_lakehouse_staging (catalog)
├── raw (schema)
├── operational (schema)
├── standardized (schema)
├── reporting (schema)
└── regulatory (schema)
```

## Data Governance (Unity Catalog)
* ✅ **5 Governed Tags**: PII, Confidential, Retention Policy, Data Classification, Business Owner
* ✅ **Row-level security**: Implemented on sensitive tables
* ✅ **Column-level masking**: PII masking via dynamic views
* ✅ **Audit logging**: 3 log tables + unified monitoring view
* ✅ **Data lineage tracking**: UC lineage + DLT event logs
* ✅ **Retention policies**: Defined in table comments

## Operations & Monitoring

### Pipeline Health
* ✅ **DLT Event Log Export**: Automated export to `operational.dlt_event_logs`
* ✅ **Pipeline Logs**: Dual-write structured logging to `operational.pipeline_logs`
* ✅ **Unified Monitoring View**: `reporting.unified_log_monitoring`

### Data Quality
* ✅ **3 DQ Monitors**: Hourly checks on key tables (trading_systems_clean, account_performance, risk_summary)
* ✅ **Alert Views**: 5 views for job failures, data freshness, pipeline errors, DQ failures, missing data
* ✅ **Alert Orchestration**: `check_and_send_alerts.py` notebook with Slack/email delivery

### Scheduled Maintenance
* ✅ **VACUUM/OPTIMIZE**: Weekly job (Sundays) for 16 tables
* ✅ **Table Lifecycle Management**: Retention policies defined
* ❌ **Automated Archival**: Purge-based archival not implemented

### Observability Gaps
* ⚠️ **Availability Monitoring**: Not explicitly configured
* ⚠️ **Cost Observability**: Alert view exists but no cost dashboards

## Orchestration (Databricks Lakeflow Jobs)

### Main Pipeline Job (ID: 254763181466712)
* **Schedule**: Daily at 6 AM UTC
* **13 Tasks**:
  1. Setup Unity Catalog
  2. API ingestion
  3. Kafka streaming
  4. Operational layer processing
  5. Standardized layer processing
  6. Reporting layer aggregation
  7. Regulatory reporting
  8. Data quality checks
  9. Alert checks
  10. Portfolio metrics
  11. Risk calculations
  12. Basel III reporting
  13. Risk reporting

### Maintenance Job
* **Schedule**: Weekly (Sundays)
* **Tasks**: VACUUM + OPTIMIZE for 16 Delta tables

## CI/CD Pipeline (GitHub Actions)
* ✅ **Workflow**: `.github/workflows/ci-cd.yml`
* ✅ **4 Jobs**: validate, deploy-dev, deploy-staging, deploy-prod
* ✅ **Bundle Deployment**: Databricks Asset Bundles (DABs)
* ✅ **PR Template**: Code review checklist

## Secret Management
* ✅ **2 Secret Scopes**: `meridian_lakehouse`, `meridian_alerts`
* ✅ **Secrets Guide**: `01_config/secrets_setup_guide.md`
* ✅ **Code Integration**: All credential access via `dbutils.secrets.get()`

## Platform Outcomes

### Achieved
* ✅ **Scalable Processing**: Serverless compute for all workloads
* ✅ **Governed Data**: Unity Catalog with tags, masking, lineage
* ✅ **Low-latency Analytics**: Serverless SQL Warehouse

### Gaps
* ❌ **High Availability**: No multi-region setup, disaster recovery, or failover
* ⚠️ **Cost Efficiency**: Using serverless but no cost dashboards or optimization reports

## Known Architectural Differences from Target Design

1. **Single Catalog vs. Multiple Lakehouses**: Financial Analytics and Risk & Compliance are schemas in one catalog, not separate lakehouses (provides less isolation)
2. **Limited Ingestion Patterns**: Only API, Kafka, and Auto Loader implemented (6 patterns in target design)
3. **Consumption Layer**: Minimal BI dashboards and interactive reporting
4. **Data Sources**: Only 2 of 5 data sources actively connected
5. **Lifecycle Management**: Retention defined but automated archival not implemented
