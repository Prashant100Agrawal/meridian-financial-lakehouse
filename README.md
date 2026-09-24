# Meridian Financial Lakehouse

A comprehensive financial data lakehouse implementation on Databricks using the medallion architecture (Bronze, Silver, Gold).

## Project Overview

This project demonstrates a production-ready financial data platform with:
- 📥 **API ingestion** every 15 minutes
- 🏗️ **Medallion architecture** (Bronze → Silver → Gold)
- 🔐 **Unity Catalog** governance
- 📊 **Analytics-ready** gold tables
- 🎯 **Data quality** monitoring
- 🚀 **Job orchestration**

## Architecture

```
Financial Data Sources
    ↓
Raw Layer (15-min API calls)
    ↓
Bronze Layer (Immutable raw data)
    ↓
Silver Layer (Cleansed & conformed)
    ↓
Gold Layer (Business aggregates)
    ↓
Consumption (Dashboards & Analytics)
```

## Folder Structure

```
00_setup/           - Catalog and schema setup
01_config/          - Configuration files
02_ingestion/       - API ingestion and mock data
03_bronze/          - Bronze layer processing
04_silver/          - Silver layer transformations
05_gold/            - Gold layer aggregations
06_data_quality/    - Data quality rules and monitoring
07_orchestration/   - Job definitions
08_analytics/       - SQL queries and dashboards
09_utilities/       - Common functions and utilities
10_tests/           - Unit tests
docs/               - Documentation
```

## Quick Start

1. **Setup**: Run notebooks in `00_setup/`
2. **Generate Data**: Run `02_ingestion/mock_data_generator.py`
3. **Ingest**: Run `02_ingestion/api_ingest_raw.py`
4. **Process**: Run bronze → silver → gold notebooks
5. **Analyze**: Use SQL queries in `08_analytics/`

See [Setup Guide](docs/setup_guide.md) for detailed instructions.

## Key Features

* **Automated ingestion**: API calls every 15 minutes
* **Schema evolution**: Handle schema changes gracefully
* **Data quality**: Built-in validation and monitoring
* **Audit trail**: Full lineage and tracking
* **Scalable**: Uses Delta Lake for ACID transactions

## Documentation

* [Architecture](docs/architecture.md)
* [Data Dictionary](docs/data_dictionary.md)
* [Setup Guide](docs/setup_guide.md)

## Tech Stack

* **Platform**: Databricks
* **Compute**: Serverless
* **Storage**: Delta Lake
* **Governance**: Unity Catalog
* **Language**: Python, SQL

## License

This is a learning/demo project for showcasing financial lakehouse patterns.
