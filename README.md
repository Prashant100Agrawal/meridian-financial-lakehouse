# Meridian Financial Lakehouse

A production-ready financial data lakehouse on Databricks using a business-centric multi-tier architecture.

## Architecture

```
Financial Data Sources (API, Kafka, S3, CDC)
    |
    v
+-------------------+     +-------------------+
|  Batch Ingestion  |     | Streaming Ingest   |
|  (API, File)      |     | (Kafka, AutoLoad)  |
+-------------------+     +-------------------+
    |                         |
    v                         v
+-------------------------------------------+
|              RAW LAYER                     |
|  /Volumes/financial_lakehouse/raw/        |
|  (Parquet + Delta landing zone)           |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|         OPERATIONAL LAYER (Bronze)         |
|  financial_lakehouse.operational          |
|  Immutable raw data, schema evolution     |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|       STANDARDIZED LAYER (Silver)         |
|  financial_lakehouse.standardized        |
|  Cleansed, deduplicated, conformed, SCD2 |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|        REPORTING LAYER (Gold)             |
|  financial_lakehouse.reporting            |
|  Business aggregates, analytics-ready    |
|  Dim tables, P&L, NAV, performance       |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|       REGULATORY LAYER (Platinum)          |
|  financial_lakehouse.regulatory           |
|  Basel III, CCAR, IFRS 9, VaR, audit      |
+-------------------------------------------+
    |
    v
Consumption (Dashboards, Analytics, Apps)
```

## Project Structure

```
00_setup/          - Catalog, schema, volume setup
01_config/         - API config, pipeline config, secrets guide
02_ingestion/      - Batch + streaming ingestion (API, Kafka, AutoLoader, CDC)
03_operational/    - Bronze layer (trading systems, custody, external APIs)
04_standardized/   - Silver layer (cleanse, dedup, conform, SCD2, DQ)
05_reporting/      - Gold layer (analytics, portfolio, risk, regulatory)
06_data_quality/   - DQ rules, monitoring, alerts
07_pipelines/      - SDP/DLT pipelines (financial analytics, risk compliance)
08_orchestration/  - Job definitions, retry policies, workflow runner
09_analytics/      - Dashboard definitions, SQL queries
10_utilities/      - Common functions, error handling, logging, table maintenance
11_tests/          - Unit tests (ingestion, DQ, transformations)
12_governance/     - Audit logs, PII masking, retention policies, UC tags
13_monitoring/     - Alerts, SLA tracking, cost observability, DLT event export
14_regulatory/     - Basel III, CCAR, IFRS 9 regulatory reporting
```

## Production Features

### Data Pipeline
- **Unified batch + streaming** ingestion via Delta Lake
- **SDP/DLT pipelines** for declarative transformations
- **Lakeflow Jobs** orchestration with 13-task DAG, retries, timeouts
- **Weekly VACUUM/OPTIMIZE** maintenance job

### Governance & Security
- **Unity Catalog** with 5 governed tags (data_sensitivity, data_domain, pii_level, retention_period, data_owner)
- **PII masking** policies on sensitive columns
- **Row/column security** for access control
- **Secret scopes** (meridian_lakehouse, meridian_alerts) for credentials
- **90-day audit log** summary table

### Monitoring & Alerting
- **DQ monitors** on key tables (hourly freshness, completeness, volume)
- **5 alert views**: pipeline failures, SLA violations, freshness, DQ, cost
- **Slack + email** alert delivery via check_and_send_alerts notebook
- **Structured pipeline logging** to Delta table (financial_lakehouse.operational.pipeline_logs)
- **DLT event log** export for long-term monitoring

### CI/CD
- **Declarative Automation Bundle** (databricks.yml) with dev/staging/prod targets
- **GitHub Actions** workflow for automated validate + deploy
- **Pull request template** for code review

### Regulatory Compliance
- **Basel III** capital adequacy ratios (Tier 1, Total Capital, Leverage)
- **CCAR** stress testing (Baseline, Adverse, Severely Adverse, Supervisory)
- **IFRS 9** Expected Credit Loss (3-stage model)
- **Regulatory VaR** at 99% confidence
- **Immutable audit trail** for compliance reporting

## Quick Start

1. **Configure secrets**: See `01_config/secrets_setup_guide.md`
2. **Deploy bundle**: `databricks bundle deploy -t dev`
3. **Run orchestration job**: Meridian Lakehouse Orchestration (daily 6 AM UTC)
4. **Monitor**: Check `financial_lakehouse.reporting.unified_log_monitoring` view

## Technology Stack
- Databricks (Serverless compute, SQL Warehouse, Unity Catalog)
- Delta Lake (ACID, time travel, schema evolution)
- Spark Declarative Pipelines (DLT)
- Lakeflow Jobs (orchestration)
- Declarative Automation Bundles (infrastructure-as-code)
- GitHub Actions (CI/CD)
