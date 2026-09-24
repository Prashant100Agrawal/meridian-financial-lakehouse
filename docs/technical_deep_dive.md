# Meridian Financial Lakehouse — Technical Deep Dive

**Project**: Meridian Financial Lakehouse  
**Document Type**: Comprehensive Technical Walkthrough with Design Rationale  
**Date**: September 24, 2026  
**Audience**: Data Engineers, Architects, Platform Engineers, Interview Panels  
**Purpose**: Explain every layer of this project — from design decisions through implementation details, deployment, monitoring, and operations — with the **why** behind each choice.

---

## Table of Contents

1. [Project Genesis & Vision](#1-project-genesis--vision)
2. [Architecture Philosophy: Why Domain-Oriented, Not Medallion](#2-architecture-philosophy-why-domain-oriented-not-medallion)
3. [Unity Catalog Design: Single Catalog, Multiple Schemas](#3-unity-catalog-design-single-catalog-multiple-schemas)
4. [Data Modeling Layer by Layer](#4-data-modeling-layer-by-layer)
5. [Ingestion Architecture: Three Patterns, One Goal](#5-ingestion-architecture-three-patterns-one-goal)
6. [Processing Layer: DLT vs Jobs, Streaming vs Batch](#6-processing-layer-dlt-vs-jobs-streaming-vs-batch)
7. [SCD Type 2: Tracking History in a Financial System](#7-scd-type-2-tracking-history-in-a-financial-system)
8. [Risk & Regulatory Calculations: The Math Behind Compliance](#8-risk--regulatory-calculations-the-math-behind-compliance)
9. [Logging & Monitoring Architecture: Dual-Write by Design](#9-logging--monitoring-architecture-dual-write-by-design)
10. [Data Quality Framework: Defense in Depth](#10-data-quality-framework-defense-in-depth)
11. [Governance & Security: Unity Catalog as the Control Plane](#11-governance--security-unity-catalog-as-the-control-plane)
12. [CI/CD Pipeline: From Commit to Production](#12-cicd-pipeline-from-commit-to-production)
13. [Orchestration Design: 13 Tasks in Deliberate Order](#13-orchestration-design-13-tasks-in-deliberate-order)
14. [Deployment Strategy: Three Environments, One Pipeline](#14-deployment-strategy-three-environments-one-pipeline)
15. [Maintenance & Lifecycle: Keeping Delta Tables Healthy](#15-maintenance--lifecycle-keeping-delta-tables-healthy)
16. [Monitoring & Alerting: The Observability Stack](#16-monitoring--alerting-the-observability-stack)
17. [Technical Decision Log](#17-technical-decision-log)
18. [Lessons Learned & Known Limitations](#18-lessons-learned--known-limitations)

---

## 1. Project Genesis & Vision

### Why This Project Exists

Meridian Financial is a financial services firm that needs to process trading data, market feeds, and risk metrics into actionable analytics and regulatory reports. The business requirements were clear:

* **Trading teams** need daily P&L, portfolio performance, and account-level metrics
* **Risk teams** need Basel III compliance reports, VaR calculations, and stress test results
* **Compliance teams** need audit trails, large trade monitoring, and IFRS 9 ECL calculations
* **Executive teams** need aggregated dashboards for decision-making
* **Regulators** demand audit-ready data with full lineage

### Why a Lakehouse (Not a Data Warehouse)

Traditional data warehouses (Snowflake, Redshift, Teradata) work well for structured, batch-processed analytics but struggle with:

* **Streaming data** — market data arrives in real-time; warehouses require batch micro-loads
* **Semi-structured data** — vendor files arrive in mixed CSV/XML/Parquet; warehouses need rigid schemas upfront
* **Cost at scale** — warehouse compute is always-on; you pay for idle even when no queries run
* **ML/AI workloads** — warehouses separate the ML environment from the data, requiring data movement

A **lakehouse** (Databricks + Delta Lake) solves these by combining:

* **Open storage format** (Delta Lake on S3) — your data is in open Parquet-based files, not locked in a proprietary engine
* **Unified batch + streaming** — Spark Structured Streaming and batch jobs read the same Delta tables
* **Serverless compute** — you pay only when compute runs; auto-terminate after idle
* **ML-native** — MLflow, feature store, and model serving live in the same platform

### Why Databricks Specifically

* **Unity Catalog** provides unified governance (tags, masking, row-level security, lineage) — no separate tool needed
* **Serverless compute** eliminates cluster management; engineers focus on code, not infrastructure
* **Lakeflow Spark Declarative Pipelines (SDP)** provide declarative data pipelines with automatic retries, error handling, and data quality expectations
* **Lakeflow Jobs** orchestrate multi-task workflows with dependencies, retries, and alerting
* **Declarative Automation Bundles (DABs)** enable GitOps-style infrastructure deployment

---

## 2. Architecture Philosophy: Why Domain-Oriented, Not Medallion

### The Traditional Medallion Pattern

Most Databricks projects use Bronze/Silver/Gold naming:

* **Bronze** = raw, unprocessed
* **Silver** = cleansed, conformed
* **Gold** = business-ready aggregates

This pattern is technically clean but has a problem: **nobody outside engineering knows what "Bronze" means**. A risk analyst, a compliance officer, or a portfolio manager cannot map "Silver" to their domain.

### Why We Chose Domain-Oriented Schemas

We replaced Bronze/Silver/Gold with names that directly reflect business functions:

```
Traditional:    Bronze → Silver → Gold
Ours:          Operational → Standardized → Reporting → Regulatory
               (raw)       (clean)      (analytics) (compliance)
```

**Why this matters:**

* **Self-documenting**: A compliance officer querying `financial_lakehouse.regulatory.risk_weighted_assets` immediately knows what the table contains and who owns it
* **Access control maps to domains**: We grant `USE SCHEMA` on `regulatory` only to the risk team; `reporting` goes to analytics — this is natural with domain names, awkward with "Gold"
* **Audit clarity**: When regulators ask "where does this number come from?", the schema name itself explains the transformation stage
* **Team alignment**: Each engineering team owns schemas that match their domain — ingestion team owns `operational`, risk team owns `regulatory`

### The Five Layers Explained

| Layer | Schema Name | Purpose | Who Writes | Who Reads |
|---|---|---|---|---|
| Raw | `raw` | Landing zone for external files before processing | Ingestion jobs (Auto Loader) | Nobody (transient) |
| Bronze | `operational` | Immutable raw data with ingestion metadata, CDC tracking | Ingestion pipelines | Standardized pipelines |
| Silver | `standardized` | Cleansed, deduplicated, conformed, SCD2 history | Standardized pipelines | Reporting + Regulatory pipelines |
| Gold (Analytics) | `reporting` | Business-ready aggregates for portfolio performance, P&L | Reporting pipelines | BI users, dashboards, analysts |
| Gold (Compliance) | `regulatory` | Regulatory reports, risk measures, audit trails | Regulatory pipelines | Risk team, compliance officers, auditors |

### Why a `raw` Schema Exists Alongside `operational`

The `raw` schema contains **UC Volumes** (not tables) that serve as the landing zone for files before Auto Loader picks them up. The `operational` schema contains the **Delta tables** that Auto Loader writes to.

**Why the separation?**

* Volumes store arbitrary files (CSV, Parquet, JSON) — they are the "inbox"
* Delta tables store structured, queryable data — they are the "filing cabinet"
* Auto Loader is the "mailroom" that moves from inbox to filing cabinet, inferring schema and handling evolution

This separation means we can re-process raw files without re-ingesting from source systems, and we can inspect rejected files before they enter the data pipeline.

---

## 3. Unity Catalog Design: Single Catalog, Multiple Schemas

### The Architecture Decision

The target design diagram envisioned **three separate lakehouses**:
1. Enterprise Data Hub (operational + standardized)
2. Financial Analytics (reporting)
3. Risk & Compliance (regulatory)

We implemented these as **schemas within a single catalog** (`financial_lakehouse`) instead.

### Why Single Catalog

| Factor | Separate Catalogs | Single Catalog (Our Choice) |
|---|---|---|
| Cross-schema joins | Require cross-catalog queries (slower, more complex) | Same-catalog joins (optimized) |
| Governance | Separate permission grants per catalog | Unified policy at catalog level, granular at schema level |
| Metastore management | 3 metastores to maintain | 1 metastore, simpler operations |
| Cost | 3x metastore overhead | 1x metastore overhead |
| Data lineage | UC lineage crosses catalogs but is harder to trace | Lineage within one catalog is natively visual |
| Isolation | Strong isolation between domains | Isolation via schema permissions (sufficient for our needs) |

**The tradeoff**: We sacrifice hard isolation between analytics and compliance data in exchange for simpler joins, unified governance, and lower operational overhead. For a financial firm of our size (not a global investment bank), this tradeoff is justified — the compliance data still has row-level security and column masking that prevents unauthorized access.

### Catalog Structure

```
financial_lakehouse (catalog)
├── raw (schema)                    — UC Volumes + config tables
│   ├── landing_files (volume)       — Auto Loader landing zone
│   ├── checkpoints (volume)         — Streaming checkpoint state
│   ├── monitoring_assets (volume)   — Alert configs, dashboard exports
│   ├── trading_systems_config       — Pipeline configuration table
│   └── daily_trading_summary        — (legacy, to be deprecated)
├── operational (schema)             — Bronze: raw data with metadata
│   ├── trading_systems              — Trade execution data
│   ├── market_data                   — Market price feeds
│   ├── risk_metrics                  — Raw risk calculations
│   ├── pipeline_logs                 — Structured pipeline logging
│   └── dlt_event_logs               — DLT event log export
├── standardized (schema)           — Silver: cleansed, conformed
│   ├── trading_systems_clean         — Deduplicated, validated
│   ├── trading_systems_conformed     — Business rules applied, renamed columns
│   └── risk_metrics_clean            — Validated risk data
├── reporting (schema)               — Gold: financial analytics
│   ├── daily_trading_summary         — Daily trade aggregates
│   ├── account_performance           — Account-level P&L
│   ├── risk_summary                  — Risk metric rollups
│   ├── portfolio_metrics             — Portfolio performance
│   ├── audit_log_summary             — Audit log aggregation
│   ├── unified_log_monitoring (view)  — Consolidated monitoring view
│   └── 5 alert views                  — Job failures, freshness, errors, DQ, missing data
└── regulatory (schema)              — Gold: risk & compliance
    ├── regulatory_large_trades       — Large trade monitoring
    ├── risk_weighted_assets          — Basel III RWA
    ├── capital_adequacy_ratios       — CAR calculations
    ├── liquidity_coverage_ratio      — LCR calculations
    ├── ccar_stress_results           — CCAR stress test results
    ├── ifrs9_ecl                     — IFRS 9 Expected Credit Loss
    ├── var_regulatory                — Regulatory VaR
    └── regulatory_audit_trail         — Complete compliance audit trail
```

### Staging Catalog

We maintain a separate `financial_lakehouse_staging` catalog with the same 5 schemas for pre-production validation. **Why separate catalog for staging?**

* Production data has PII masking and row-level security — staging data is unmasked for testing
* Prevents accidental cross-environment queries (you cannot `JOIN financial_lakehouse.reporting.*` from staging)
* Allows CI/CD pipeline to deploy and test in isolation before promoting to production

---

## 4. Data Modeling Layer by Layer

### Layer 1: Raw (The Inbox)

**What lives here**: UC Volumes that store files before processing.

**Volume paths**:
```
/Volumes/financial_lakehouse/raw/landing_files/trading_systems/
/Volumes/financial_lakehouse/raw/landing_files/market_data/
/Volumes/financial_lakehouse/raw/landing_files/risk_metrics/
/Volumes/financial_lakehouse/raw/checkpoints/kafka-streaming/
/Volumes/financial_lakehouse/raw/checkpoints/auto-loader/
/Volumes/financial_lakehouse/raw/monitoring_assets/
```

**Why volumes instead of tables?**

* Files arrive in mixed formats (Parquet, CSV, JSON) — Delta tables require a fixed schema
* We need to preserve the original file as-is for audit purposes (Delta tables transform data)
* Auto Loader can incrementally discover and process new files from volumes without scanning the entire directory each time (it uses RocksDB-based checkpoints)

### Layer 2: Operational (Bronze — Immutable Raw Data)

**What lives here**: Delta tables that store raw data with minimal transformation, plus ingestion metadata.

**Key design decisions**:

* **Append-only**: Bronze tables are never updated or deleted — they are an immutable record of what arrived. **Why?** In financial services, you must be able to prove what data you received and when. If a record is wrong, you fix it in the Silver layer, not by overwriting Bronze.

* **Ingestion metadata columns**: Every record gets `_ingestion_timestamp` and `_source_system` columns added by the `ingest_to_raw()` function:
  ```python
  df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp())
                       .withColumn("_source_system", lit(data_source))
  ```
  **Why?** When investigating data issues, you need to know exactly when a record landed and which source system it came from. Without this, you cannot distinguish between a source system bug and a pipeline bug.

* **Schema evolution**: Auto Loader's `cloudFiles.inferColumnTypes=true` and `cloudFiles.schemaLocation` handle schema changes automatically. **Why?** Source systems evolve — new columns appear, types change. Hardcoding schemas would break the pipeline every time a source changes. Auto Loader detects changes, evolves the schema, and continues processing.

* **Pipeline logs as a table**: `operational.pipeline_logs` is a Delta table, not just log files. **Why?** Log files are unstructured and hard to query. By dual-writing logs to a Delta table (see Section 9), we can SQL-query pipeline health, join logs to data quality results, and build alert views on top.

* **DLT event logs**: `operational.dlt_event_logs` is an export of DLT pipeline events. **Why?** DLT stores its event log in an internal location that is hard to query directly. Exporting to a Delta table makes events queryable alongside pipeline logs, enabling unified monitoring.

### Layer 3: Standardized (Silver — Cleansed, Conformed)

**What lives here**: Delta tables where data is cleansed, deduplicated, and conformed to enterprise standards.

**Three tables, three purposes**:

#### trading_systems_clean — Cleansing + Validation

Implemented in the DLT medallion pipeline with `@dlt.expect_all_or_drop`:

```python
@dlt.expect_all_or_drop({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_quantity": "quantity > 0",
    "valid_price": "price > 0"
})
def silver_trading_clean():
    return (
        dlt.read_stream("bronze_trading_systems")
        .dropDuplicates(["transaction_id"])
        .withColumn("silver_processed_timestamp", current_timestamp())
    )
```

**Why `expect_all_or_drop`?**

* In Bronze, we keep everything (even bad records) for auditability
* In Silver, we drop records that fail basic validation — a trade with NULL transaction_id or negative quantity is not a real trade
* `expect_all_or_drop` logs the dropped records (DLT tracks how many were dropped and why) so we can monitor data quality without failing the entire pipeline
* **Alternative considered**: `expect_all_or_fail` would stop the pipeline on any bad record. This is too aggressive for a financial pipeline that processes thousands of trades — one bad record should not block the entire day's processing

**Why `dropDuplicates` on `transaction_id`?**

* Source systems sometimes resend the same trade (network retries, duplicate API calls)
* Duplicate trades would inflate volume and P&L calculations
* `dropDuplicates` uses Spark's watermarking to deduplicate within a time window — trades with the same ID arriving within the same micro-batch are collapsed

#### trading_systems_conformed — Business Rules + Column Standardization

```python
def silver_trading_conformed():
    return (
        dlt.read("silver_trading_clean")
        .withColumn("txn_id", col("transaction_id"))
        .withColumn("acct_id", col("account_id"))
        .withColumn("symbol", col("instrument_id"))
        .withColumn("side", col("transaction_type"))
        .withColumn("total_value", col("quantity") * col("price"))
        .withColumn("is_large_trade", when(col("total_value") > 100000, True).otherwise(False))
        ...
    )
```

**Why rename columns?**

* Source systems use their own naming conventions (`transaction_id`, `account_id`, `instrument_id`)
* Downstream consumers (reporting, regulatory) need consistent, short column names (`txn_id`, `acct_id`, `symbol`)
* This is the **conformance layer** — the single place where source-specific names map to enterprise-standard names
* If a source system renames a column, only this conformed table changes; all downstream tables are unaffected

**Why `total_value = quantity * price`?**

* This is a derived business metric used everywhere — P&L, risk, regulatory reporting
* Computing it once in Silver ensures consistency — if the formula changes (e.g., adding fees), we change it in one place

**Why `is_large_trade` flag?**

* Regulatory reporting requires monitoring trades above $100,000
* Computing this flag in Silver means downstream regulatory pipelines can simply filter `WHERE is_large_trade = true` instead of recomputing the threshold
* The threshold ($100,000) is hardcoded now; in production, this would be a config table value

#### risk_metrics_clean — Validated Risk Data

Same pattern: deduplicate, validate non-null risk metric values, add processing timestamp. Separate from trading data because risk metrics have different validation rules and different consumers.

#### silver_scd2.py — Slowly Changing Dimensions

See Section 7 for the full SCD Type 2 explanation.

### Layer 4: Reporting (Gold — Financial Analytics)

**What lives here**: Business-ready aggregates that trading teams, portfolio managers, and executives query directly.

**Key tables and their purpose**:

#### daily_trading_summary

Aggregates trades by date, account, instrument, and currency:

```python
.groupBy("trade_date", "acct_id", "symbol", "ccy")
.agg(
    count("*").alias("trade_count"),
    sum(when(col("side") == "BUY", col("qty")).otherwise(0)).alias("buy_volume"),
    sum(when(col("side") == "SELL", col("qty")).otherwise(0)).alias("sell_volume"),
    sum(when(col("side") == "BUY", col("total_value")).otherwise(0)).alias("buy_value"),
    sum(when(col("side") == "SELL", col("total_value")).otherwise(0)).alias("sell_value"),
    avg("unit_price").alias("avg_price"),
    min("unit_price").alias("min_price"),
    max("unit_price").alias("max_price")
)
.withColumn("net_position", col("buy_volume") - col("sell_volume"))
.withColumn("net_value", col("sell_value") - col("buy_value"))
```

**Why these specific aggregates?**

* **trade_count**: Volume metric — how active was this account today?
* **buy_volume / sell_volume**: Position tracking — is the account net long or short?
* **buy_value / sell_value**: P&L components — how much money moved in each direction?
* **avg_price / min_price / max_price**: Price range — was the trading volatile?
* **net_position**: Quick answer to "what's this account's net exposure?"
* **net_value**: Quick answer to "did this account make or lose money?"

**Why group by currency?**

* Trades in different currencies cannot be aggregated (100 USD + 100 EUR is meaningless)
* Grouping by currency ensures all aggregates are within a single currency
* Currency conversion happens in a separate step (not yet implemented — see gap analysis)

#### account_performance

Account-level metrics including NAV, returns, and attribution. This is the table portfolio managers query every morning to see their performance.

#### portfolio_metrics

Portfolio-level rollups across accounts. Used for executive dashboards and firm-wide reporting.

#### risk_summary

Pre-calculated risk metrics at the account level — VaR, exposure, concentration. Separated from the regulatory schema because this is for internal risk management, not regulatory reporting.

#### audit_log_summary + unified_log_monitoring

These are observability tables (see Section 9) — they aggregate pipeline logs into queryable summaries.

### Layer 5: Regulatory (Gold — Risk & Compliance)

**What lives here**: Regulatory reporting tables that compliance officers and auditors use directly.

**Why a separate schema from reporting?**

* **Different consumers**: Reporting is for portfolio managers; regulatory is for compliance officers and regulators
* **Different retention**: Regulatory tables must be retained for 7+ years (regulatory requirement); reporting tables can be purged after 2 years
* **Different access patterns**: Regulatory tables are queried periodically (quarterly reports, audits); reporting tables are queried continuously
* **Different security**: Regulatory tables have stricter row-level security (only compliance team can see all rows)

See Section 8 for the detailed regulatory calculations.

---

## 5. Ingestion Architecture: Three Patterns, One Goal

### The Three Implemented Patterns

```
Pattern 1: API Ingestion (Batch with Retry)
  Source → REST API → APIIngestionClient → Parquet → raw/landing_files → Auto Loader → operational

Pattern 2: Kafka Streaming (Real-Time)
  Source → AWS MSK (Kafka) → Structured Streaming → operational (via DLT)

Pattern 3: File-Based Ingestion (Auto Loader)
  Source → S3 Upload → raw/landing_files → Auto Loader → operational (via DLT)
```

### Pattern 1: API Ingestion — Why and How

**Why API ingestion?**

Trading systems and market data providers expose REST APIs. We need to poll these APIs periodically, handle rate limits, and store results.

**Implementation** (`02_ingestion/api_ingest_raw.py`):

```python
class APIIngestionClient:
    def __init__(self, base_url, max_retries=3, backoff_factor=0.5):
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
```

**Why each design choice**:

* **Retry with exponential backoff**: APIs fail transiently — 429 (rate limit), 503 (service unavailable) are temporary. Retrying with increasing wait time (0.5s, 1s, 2s) handles these without manual intervention.

* **`status_forcelist=[429, 500, 502, 503, 504]`**: These are the HTTP status codes that indicate transient failures. 4xx codes like 401 (auth) or 404 (not found) are not retried because they are permanent failures.

* **`allowed_methods=["HEAD", "GET", "OPTIONS"]`**: Only idempotent methods are retried. POST/PUT/DELETE are not retried automatically because they might create duplicate side effects.

* **Rate limiting decorator**:
  ```python
  def rate_limit(calls_per_second=10):
      min_interval = 1.0 / calls_per_second
  ```
  **Why?** External APIs enforce rate limits (e.g., 10 requests/second). Exceeding these limits results in 429 errors. Our rate limiter ensures we stay within limits by enforcing a minimum interval between calls.

* **Paginated fetch**: `fetch_paginated_data()` handles APIs that return data in pages. **Why?** Some API endpoints return thousands of records — fetching them all at once would timeout. Pagination fetches in chunks of 100, making each request fast and reliable.

* **Write to Parquet (not Delta) in raw**: The `ingest_to_raw()` function writes to `/Volumes/financial_lakehouse/raw/landing_files/` as Parquet. **Why Parquet, not Delta?** Because the raw landing zone is a volume (file system), not a Delta table. Auto Loader handles the conversion to Delta when it picks up the files.

* **Ingestion metadata**: Each record gets `_ingestion_timestamp` and `_source_system`. **Why?** If we need to reprocess or debug, we know exactly when the data arrived and from where.

### Pattern 2: Kafka Streaming — Why and How

**Why Kafka?**

Market data feeds (Bloomberg, Refinitiv) and trade execution systems stream events in real-time. Polling APIs would introduce latency (minutes); Kafka delivers events in milliseconds.

**Implementation** (`07_pipelines/dlt_streaming_ingestion.py`):

```python
@dlt.table(name="trading_systems_stream")
def trading_systems_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")
        .withColumn("stream_ingestion_time", current_timestamp())
        .withColumn("stream_source", lit("trading_systems"))
    )
```

**Why Auto Loader (`cloudFiles`) instead of direct Kafka consumer?**

* Auto Loader provides **exactly-once semantics** — even if the pipeline restarts, no records are lost or duplicated
* Auto Loader handles **schema inference and evolution** — if the source adds a column, Auto Loader detects it and evolves the schema without pipeline changes
* Auto Loader uses **incremental file discovery** — it maintains a RocksDB checkpoint to track which files have been processed, so it does not re-scan the entire directory on each trigger
* Direct Kafka consumers require manual checkpoint management, offset tracking, and schema handling — all of which Auto Loader provides for free

**Why `maxFilesPerTrigger=1000`?**

* Controls the batch size per micro-batch — too high causes memory pressure, too low causes excessive small batches
* 1000 files per trigger is a reasonable default for Parquet files averaging 1-10 MB each
* This is tunable — in production with higher volume, we would increase this to 5000-10000

**Why three separate stream tables (trading, market_data, risk_metrics)?**

* Each stream has different schema, different latency requirements, and different consumers
* Separate DLT tables allow independent scaling — if market data volume spikes, it does not block trading data processing
* Separate tables also simplify monitoring — we can see the lag for each stream independently

### Pattern 3: File-Based Auto Loader — Why and How

**Why file-based?**

Vendor files (custody positions, reconciliation files) arrive as scheduled CSV/XML/Parquet drops to S3. These are not streaming — they arrive once a day or once a week. Auto Loader handles both streaming (Kafka → S3) and batch (scheduled file drops) with the same API.

**The beauty of Auto Loader**: The same `spark.readStream.format("cloudFiles")` pattern works for both real-time streaming and batch file drops. Auto Loader detects new files whenever they arrive and processes them. There is no separate batch pipeline — it is all unified.

---

## 6. Processing Layer: DLT vs Jobs, Streaming vs Batch

### Why Lakeflow Spark Declarative Pipelines (SDP)

The project uses DLT (via `@dlt.table` decorators) for the core medallion pipeline and standard notebook jobs for regulatory calculations.

**Why DLT for Bronze/Silver/Gold?**

| DLT Advantage | Why It Matters |
|---|---|
| **Declarative** | You define what the output should look like, not how to compute it. DLT handles dependency ordering, incremental processing, and checkpointing. |
| **Automatic retries** | If a task fails, DLT retries automatically. Standard jobs require manual retry logic. |
| **Data quality expectations** | `@dlt.expect_all_or_drop` integrates DQ checks into the pipeline — no separate validation step. |
| **Lineage tracking** | DLT automatically records which tables depend on which, visible in the DLT UI. |
| **Incremental processing** | `read_stream` enables incremental processing — only new data is processed, not full table scans. |
| **Schema evolution** | Auto Loader + DLT handle schema changes without pipeline modifications. |

**Why standard notebook jobs for regulatory calculations?**

* Regulatory calculations (Basel III, IFRS 9, VaR) require complex SQL with CTEs, window functions, and multi-step logic
* DLT is optimized for streaming, incremental pipelines — regulatory calculations are batch (daily, not continuous)
* Standard notebook jobs give full control over SQL execution, which is important for regulatory accuracy
* DLT does not support all SQL constructs needed for regulatory calculations (complex CTEs, subqueries)

### The DLT Medallion Pipeline (`dlt_lakehouse1_medallion.py`)

```python
# Bronze: Raw ingestion
@dlt.table(name="bronze_trading_systems", table_properties={"quality": "bronze"})
def bronze_trading_systems():
    return spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/financial_lakehouse/raw/landing_files/trading_systems")

# Silver: Cleansing + validation
@dlt.table(name="silver_trading_clean")
@dlt.expect_all_or_drop({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_quantity": "quantity > 0",
    "valid_price": "price > 0"
})
def silver_trading_clean():
    return dlt.read_stream("bronze_trading_systems").dropDuplicates(["transaction_id"])

# Silver: Conformance
@dlt.table(name="silver_trading_conformed")
def silver_trading_conformed():
    return dlt.read("silver_trading_clean")
        .withColumn("total_value", col("quantity") * col("price"))
        .withColumn("is_large_trade", when(col("total_value") > 100000, True).otherwise(False))

# Gold: Aggregation
@dlt.table(name="gold_daily_trading_summary")
def gold_daily_trading_summary():
    return dlt.read("silver_trading_conformed")
        .groupBy("trade_date", "acct_id", "symbol", "ccy")
        .agg(count("*").alias("trade_count"), ...)
```

**Why `read_stream` for Bronze/Silver but `read` for Gold?**

* **Bronze** uses `readStream` — it continuously ingests new files from the landing zone
* **Silver (clean)** uses `read_stream` — it processes new Bronze records incrementally
* **Silver (conformed)** uses `read` (batch) — conformance is a full-table operation because it renames columns and applies business rules to the entire dataset
* **Gold** uses `read` (batch) — aggregation requires grouping all records for a given date, which is a batch operation

**Why this ordering matters**: DLT automatically detects dependencies from `dlt.read()` / `dlt.read_stream()` calls and executes them in the correct order. If Bronze fails, Silver and Gold are not attempted. This prevents garbage data from propagating downstream.

### Why `table_properties` with quality tags

```python
@dlt.table(name="bronze_trading_systems", table_properties={"quality": "bronze"})
```

* The `quality` property tags the table for monitoring — DLT can filter and alert based on quality level
* `pipelines.autoOptimize.zOrderCols` tells DLT to automatically Z-order the `transaction_id` column, which optimizes point lookups

---

## 7. SCD Type 2: Tracking History in a Financial System

### Why SCD Type 2 at All

In financial services, account attributes change over time:

* An account's status changes from ACTIVE to SUSPENDED
* An account's owner changes
* An account's balance threshold changes

If we simply overwrite the record, we lose the history. If regulators ask "what was this account's status on March 15?", we cannot answer.

**SCD Type 2** solves this by keeping all versions of a record, with effective dating:

```
account_id | status  | effective_start | effective_end | is_current
-----------|---------|-----------------|---------------|----------
ACC001     | ACTIVE  | 2026-01-01      | 2026-03-14    | false
ACC001     | SUSPENDED| 2026-03-15     | NULL          | true
```

### Implementation (`04_standardized/silver_scd2.py`)

**Key design decisions**:

* **MD5 hash for change detection**: We compute `record_hash = md5(concat_ws("|", col1, col2, ...))` for tracked columns. **Why MD5?** It is fast and sufficient for change detection (not for security). If the hash of the new record differs from the stored hash, a tracked attribute changed.

* **Surrogate key**: `business_key_value + "_" + effective_start_date` creates a unique key for each version. **Why not a UUID?** The composite key is human-readable and debuggable — you can tell when a version started just by looking at the key.

* **MERGE pattern**: The `process_scd2()` function uses Delta `MERGE` to:
  1. Close out changed records (set `effective_end_date` and `is_current = false`)
  2. Insert new versions of changed records
  3. Insert brand-new records

**Why Delta MERGE instead of INSERT + UPDATE?**

* `MERGE` is atomic — it either completes all changes or none, preventing partial updates
* Delta Lake's `MERGE` is optimized for this exact pattern (upsert with conditional actions)
* Separate INSERT + UPDATE would require two operations with potential race conditions

**Configurable tracked columns**:

```python
scd2_tables = {
    "accounts": {
        "source": "financial_lakehouse.standardized.accounts",
        "target": "financial_lakehouse.standardized.accounts_scd2",
        "business_key": ["account_id"],
        "tracked_columns": ["account_name", "account_type", "status", "balance", "owner"],
        "exclude_from_hash": ["silver_insert_timestamp", "silver_update_timestamp"]
    }
}
```

**Why configurable?**

* Different tables need different columns tracked — account status is tracked but `silver_insert_timestamp` is not (it changes every run but is not a business attribute)
* `exclude_from_hash` prevents false change detections from metadata columns that update on every run
* Adding a new SCD2 table is a config change, not a code change

---

## 8. Risk & Regulatory Calculations: The Math Behind Compliance

### Why This Section Matters

Regulatory calculations are not just data engineering — they are mathematical formulas mandated by law. Getting these wrong means regulatory fines. Each calculation must be traceable, reproducible, and auditable.

### VaR (Value at Risk) Calculation (`dlt_risk_compliance.py`)

```sql
WITH daily_returns AS (
    SELECT acct_id, trade_date, net_pnl as daily_return, currency
    FROM financial_analytics.nav_pnl.profit_loss
),
portfolio_stats AS (
    SELECT 
        acct_id, currency,
        AVG(daily_return) as mean_return,
        STDDEV(daily_return) as std_dev,
        COUNT(*) as observation_days
    FROM daily_returns
    GROUP BY acct_id, currency
)
SELECT 
    acct_id, currency,
    mean_return - (1.645 * std_dev) as var_95,
    mean_return - (2.326 * std_dev) as var_99,
    mean_return - (2.5 * std_dev) as expected_shortfall,
    CASE WHEN std_dev > 0 THEN mean_return / std_dev ELSE 0 END as sharpe_ratio
FROM portfolio_stats
```

**Why these specific values?**

* **1.645**: Z-score for 95% confidence interval (one-tailed). This means we are 95% confident that losses will not exceed `var_95`.
* **2.326**: Z-score for 99% confidence interval. Regulatory bodies (Basel III) require 99% VaR.
* **2.5**: Approximate multiplier for Expected Shortfall (ES), which is the average loss given that the loss exceeds VaR. Basel III is moving from VaR to ES because ES captures tail risk better.
* **Sharpe ratio**: `mean_return / std_dev` measures risk-adjusted return. Not regulatory, but included because risk teams need it.

**Why this is a parametric (variance-covariance) approach?**

* The simplest VaR method — assumes returns are normally distributed
* Fast to compute (just mean and standard deviation)
* **Limitation**: Does not capture fat tails or skewness — in practice, financial returns are not normal. For production, we would use historical simulation or Monte Carlo. The parametric approach is the starting point.

**Why per-account and per-currency?**

* VaR is meaningless when aggregated across currencies (you cannot add USD VaR and EUR VaR)
* Per-account VaR allows risk teams to identify which accounts carry the most risk

### Large Trade Monitoring

```python
def large_trade_report():
    return (
        trading_df
        .filter(col("trade_date") >= date_sub(current_date(), 30))  # Last 30 days
        .withColumn("regulatory_category",
            when(col("total_value") > 100000, lit("HIGH_VALUE"))
            .when(col("total_value") > 50000, lit("MEDIUM_VALUE"))
            .otherwise(lit("STANDARD")))
        .withColumn("compliance_status",
            when((col("is_large_trade") == True) & (col("total_value") > 100000),
                 lit("REQUIRES_REVIEW"))
            .otherwise(lit("AUTO_APPROVED")))
    )
```

**Why 30-day rolling window?**

* Regulatory bodies require monitoring of large trades over a rolling window
* 30 days is the standard window for MiFID II large trade reporting
* The filter ensures we do not re-process historical trades unnecessarily

**Why three categories (HIGH, MEDIUM, STANDARD)?**

* Different categories trigger different compliance workflows
* HIGH_VALUE trades ($100K+) require manual compliance review
* MEDIUM_VALUE trades ($50K+) are logged for trend analysis
* STANDARD trades are auto-approved but still recorded for audit

### Audit Trail

```python
.withColumn("processing_lag_seconds",
    (unix_timestamp(col("processed_dt")) - unix_timestamp(col("trade_dt"))))
.withColumn("processing_status",
    when(col("processing_lag_seconds") > 60, lit("DELAYED"))
    .otherwise(lit("ON_TIME")))
```

**Why processing lag tracking?**

* Regulators ask not just "what happened" but "when did you know?"
* If a large trade was executed at 10:00 AM but not processed until 2:00 PM, that 4-hour lag is a compliance concern
* The 60-second threshold is conservative — in a real-time streaming pipeline, lag should be seconds, not minutes

### Other Regulatory Tables

| Table | Formula/Logic | Regulatory Framework |
|---|---|---|
| `risk_weighted_assets` | RWA = Sum(asset_value * risk_weight) for each asset class | Basel III |
| `capital_adequacy_ratios` | CAR = Tier 1 Capital / RWA; must be >= 8% | Basel III |
| `liquidity_coverage_ratio` | LCR = HQLA / Net Cash Outflows (30-day); must be >= 100% | Basel III |
| `ccar_stress_results` | Stress test P&L under adverse scenarios (severe, moderate, baseline) | Dodd-Frank / CCAR |
| `ifrs9_ecl` | Expected Credit Loss = PD * LGD * EAD (12-month or lifetime) | IFRS 9 |
| `var_regulatory` | 99% VaR using historical simulation (not parametric) | Basel III |
| `regulatory_audit_trail` | Who accessed what, when, with what query | SOX / BCBS 239 |

---

## 9. Logging & Monitoring Architecture: Dual-Write by Design

### The Problem with Traditional Logging

Most pipelines log to stdout or log files. These are:
* **Not queryable** — you cannot `SELECT * FROM logs WHERE level = 'ERROR'`
* **Not joinable** — you cannot join logs to data quality results or job runs
* **Not retained** — cluster logs disappear when the cluster terminates
* **Not alertable** — you cannot trigger an alert when an error appears in a log file

### Our Solution: Dual-Write Logging

The `PipelineLogger` class (`10_utilities/logging_utils.py`) writes to **both** console (stdout) and a Delta table (`operational.pipeline_logs`):

```python
class PipelineLogger:
    def _write_to_delta(self, level, message, step_name=None,
                        records_processed=None, duration_seconds=None,
                        error_details=None):
        log_row = Row(
            log_timestamp=datetime.now(),
            pipeline_name=self.pipeline_name,
            log_level=level,
            message=message,
            step_name=step_name,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            error_details=error_details,
            run_id=self.run_id,
            created_by=spark.sql("SELECT current_user()").collect()[0][0]
        )
        df = spark.createDataFrame([log_row])
        df.write.mode("append").saveAsTable(PIPELINE_LOG_TABLE)
```

**Why dual-write?**

* **Console output** is for real-time debugging — you see it in the Databricks job run UI while the job is running
* **Delta table** is for historical analysis — you can query it days later, join to other tables, build alert views
* If one fails, the other still works — logging failures do not break the pipeline

**Why the `try/except` wrapper in `_write_to_delta`?**

```python
try:
    df.write.mode("append").saveAsTable(PIPELINE_LOG_TABLE)
except Exception as e:
    print(f"Failed to write log to Delta: {e}")
```

* Logging is a cross-cutting concern — it should never crash the pipeline
* If the Delta table is temporarily unavailable (e.g., catalog issue), the log goes to console only and the pipeline continues
* The exception is caught and printed, not re-raised

**Why `run_id`?**

* Every pipeline run gets a unique `run_id` (e.g., `20260924_060000`)
* This allows filtering: `SELECT * FROM pipeline_logs WHERE run_id = '20260924_060000'`
* Without `run_id`, you could not distinguish logs from different runs of the same pipeline

**Why `step_name` and `duration_seconds`?**

* `step_start()` and `step_end()` methods track how long each step took
* This enables performance monitoring: `SELECT step_name, AVG(duration_seconds) FROM pipeline_logs GROUP BY step_name`
* If a step that normally takes 30 seconds suddenly takes 300 seconds, that is a signal of a problem

**Why `records_processed`?**

* Tracks data volume per step — if a step that normally processes 50,000 records processes 0, that is a data freshness issue
* Enables alerts: "Step X processed 0 records — possible source system outage"

### The Log Schema

```
operational.pipeline_logs
├── log_timestamp        TIMESTAMP    — when the log was written
├── pipeline_name        STRING      — which pipeline wrote this log
├── log_level           STRING      — INFO, WARNING, ERROR
├── message             STRING      — human-readable log message
├── step_name           STRING      — which step within the pipeline
├── records_processed   BIGINT      — how many records were processed
├── duration_seconds    DOUBLE      — how long the step took
├── error_details       STRING      — stack trace or error details (for ERROR level)
├── run_id              STRING      — unique run identifier
└── created_by          STRING      — who/what ran the pipeline
```

### DLT Event Log Export

DLT pipelines generate their own event log (flow progress, data quality metrics, errors). This log is stored internally and is hard to query directly. We export it to `operational.dlt_event_logs` for unified monitoring.

**Why export?**

* DLT event log is in a JSON file format with complex nesting — not SQL-friendly
* Exporting to a Delta table flattens the JSON and makes it queryable
* We can join DLT events with pipeline logs: `pipeline_logs JOIN dlt_event_logs ON run_id`

### Unified Monitoring View

`reporting.unified_log_monitoring` is a SQL view that consolidates pipeline logs and DLT events:

```sql
-- Conceptual structure
SELECT 
    log_timestamp, pipeline_name, log_level, message, step_name,
    records_processed, duration_seconds, run_id,
    'PIPELINE_LOG' as log_source
FROM financial_lakehouse.operational.pipeline_logs
UNION ALL
SELECT 
    event_timestamp, pipeline_name, event_type, event_message, flow_name,
    records_processed, duration_seconds, run_id,
    'DLT_EVENT' as log_source
FROM financial_lakehouse.operational.dlt_event_logs
```

**Why a view, not a table?**

* A view is always up-to-date — no refresh needed
* A view costs nothing when not queried — no compute until someone runs a SELECT
* Multiple alert views can be built on top of this unified view

---

## 10. Data Quality Framework: Defense in Depth

### The Three Layers of DQ

```
Layer 1: Inline DQ (DLT Expectations)
  → @dlt.expect_all_or_drop in the Silver layer
  → Drops invalid records, logs what was dropped
  → Prevents bad data from entering Silver

Layer 2: DQ Monitors (Scheduled Checks)
  → 3 monitors running hourly on key tables
  → Checks: null counts, row counts, freshness, value ranges
  → Stores results in system tables

Layer 3: Alert Views (SQL-based Detection)
  → 5 views that detect specific failure patterns
  → Triggers alert orchestration notebook
  → Sends Slack/email notifications
```

### Why Three Layers?

* **Layer 1 (inline)** is the first line of defense — bad data never enters the Silver layer. This is fast but only catches simple validation issues (nulls, negative values).

* **Layer 2 (monitors)** catches more complex issues — data freshness (did new data arrive today?), statistical anomalies (is the row count 50% below average?). These run hourly because they need to compare across runs, not just within a single batch.

* **Layer 3 (alerts)** is the human notification layer — if monitors detect an issue, alert views generate the specific alert message and the orchestration notebook sends it to Slack/email. This ensures someone knows about the problem within minutes.

### The 5 Alert Views

| View | What It Detects | Query Logic |
|---|---|---|
| `alert_job_failures` | Pipeline jobs that failed in the last 24 hours | `WHERE log_level = 'ERROR' AND log_timestamp > now() - 24 hours` |
| `alert_data_freshness` | Tables that have not been updated recently | `WHERE max(updated_at) < now() - expected_interval` |
| `alert_pipeline_errors` | Non-fatal pipeline errors (warnings) | `WHERE log_level = 'WARNING' AND step_name IS NOT NULL` |
| `alert_dq_failures` | DQ monitor failures | `WHERE monitor_status = 'FAIL'` |
| `alert_missing_data` | Steps that processed 0 records | `WHERE records_processed = 0 AND step_name IS NOT NULL` |

### Alert Orchestration (`check_and_send_alerts.py`)

This notebook runs as a task in the main pipeline job. It:

1. Queries all 5 alert views
2. For each row returned (i.e., each active alert):
   * Formats an alert message
   * Sends to Slack via webhook (stored in `meridian_alerts` secret scope)
   * Sends email via SMTP (credentials in `meridian_alerts` secret scope)
3. Logs the alert delivery to `pipeline_logs`

**Why Slack + email (not just one)?**

* Slack is for the engineering team — fast, real-time, in-channel
* Email is for stakeholders who do not monitor Slack (compliance officers, executives)
* Both are sent so that someone always sees the alert, regardless of which tool they monitor

---

## 11. Governance & Security: Unity Catalog as the Control Plane

### Why Unity Catalog (Not Hive Metastore)

Hive metastore (the legacy Databricks metastore) provides table metadata but no governance:

* No column-level masking
* No row-level security
* No governed tags
* No audit logging at the table/column level
* No lineage tracking

Unity Catalog provides all of these, making it the single governance control plane.

### The 5 Governed Tags

| Tag | Purpose | Example Usage |
|---|---|---|
| `PII` | Personally Identifiable Information | Tagged on columns containing account holder names, SSNs |
| `Confidential` | Business-sensitive data | Tagged on trading strategy tables |
| `Retention Policy` | How long to keep data | Tagged on regulatory tables: "7 years" |
| `Data Classification` | Data sensitivity level | Tagged: "Public", "Internal", "Restricted" |
| `Business Owner` | Who owns this data | Tagged: "Risk Team", "Trading Team" |

**Why governed tags (not free-form tags)?**

* Governed tags enforce a controlled vocabulary — you cannot invent new tags; you must use the predefined keys
* This ensures consistency — every table with PII is tagged the same way
* Governed tags can trigger automated policies (e.g., "all tables tagged PII must have column masking")

### Row-Level Security

Implemented via dynamic views:

```sql
CREATE VIEW reporting.account_performance_secure AS
SELECT * FROM financial_lakehouse.reporting.account_performance
WHERE acct_id IN (
    SELECT account_id FROM financial_lakehouse.operational.user_account_access
    WHERE user_id = current_user()
)
```

**Why dynamic views (not Delta table row filters)?**

* Dynamic views were available before Delta row filters were GA
* They are simpler to understand — a view with a WHERE clause
* Row filters are now the recommended approach and we plan to migrate

### Column-Level Masking

PII columns (account holder names, personal identifiers) are masked:

```sql
CREATE VIEW reporting.account_performance_masked AS
SELECT
    acct_id,
    CASE
        WHEN is_member('risk_team') THEN account_holder_name
        ELSE '***REDACTED***'
    END as account_holder_name,
    ...
FROM financial_lakehouse.reporting.account_performance
```

**Why `is_member()` instead of hardcoded user check?**

* `is_member('risk_team')` checks if the current user is in the `risk_team` group
* This is group-based, not user-based — when team membership changes, the masking automatically adjusts
* Hardcoding user emails would require updating the view every time someone joins or leaves the team

### Audit Logging

Three log tables + one unified view:
* `operational.pipeline_logs` — pipeline execution logs
* `operational.dlt_event_logs` — DLT pipeline events
* `reporting.audit_log_summary` — aggregated audit information
* `reporting.unified_log_monitoring` (view) — consolidated monitoring

**Why three tables?**

* Different log sources have different schemas — pipeline logs have step names and durations; DLT events have flow names and data quality metrics
* Forcing them into one table would require many nullable columns
* Separate tables with a unified view gives the best of both: clean schemas + unified querying

---

## 12. CI/CD Pipeline: From Commit to Production

### The Pipeline

```
Developer writes code in a feature branch
  → Pushes to GitHub
  → Opens a Pull Request to main
  → GitHub Actions CI triggers:
     1. validate: Lint Python, check SQL syntax, validate DABs bundle, run unit tests
     2. deploy-dev: Deploy DABs to dev workspace, run integration tests
  → PR is reviewed and merged to main
  → deploy-staging: Deploy DABs to staging workspace, run smoke tests (triggered by release tag)
  → deploy-prod: Deploy DABs to prod workspace (requires manual approval)
```

### Why GitHub Actions (Not Jenkins or Databricks Git Integration Alone)

* **GitHub Actions** is native to GitHub — no separate CI server to maintain
* **DABs CLI** (`databricks bundle validate/deploy`) integrates with GitHub Actions via the `databricks/setup-databricks` action
* Jenkins would require a self-hosted runner, more maintenance, and a separate UI
* Databricks Git Integration alone can pull code but cannot run tests or validate bundles — you need a CI engine

### Why DABs (Not Manual Job Creation)

* **Declarative**: `databricks.yml` defines the entire infrastructure (jobs, pipelines, variables) as code
* **Versioned**: Every change goes through Git — you can see who changed what and when
* **Reproducible**: `databricks bundle deploy --target prod` always deploys exactly what is in the YAML — no manual console changes
* **Multi-environment**: The same `databricks.yml` deploys to dev, staging, and prod with different variables per target

### The `databricks.yml` Structure

```yaml
bundle:
  name: meridian-financial-lakehouse

variables:
  # Environment-specific variables defined per target

targets:
  dev:
    # Dev-specific: smaller compute, dev catalog
  staging:
    # Staging-specific: staging catalog, smoke tests
  prod:
    # Prod-specific: production catalog, full schedule

resources:
  jobs:
    main_pipeline:  # 13-task daily pipeline
    maintenance:    # Weekly VACUUM/OPTIMIZE
  pipelines:
    lakehouse_pipeline:  # DLT medallion pipeline
```

**Why define jobs AND pipelines in the bundle?**

* Jobs orchestrate the overall workflow (task 1 runs, then task 2, etc.)
* DLT pipelines handle the streaming/incremental processing within that workflow
* Both need to be versioned and deployed together — if we change the DLT pipeline code, the job that triggers it should also be updated

### Why Manual Approval for Production

* Production deployment impacts live data and regulatory reports
* A bad deployment could corrupt Delta tables, fail regulatory calculations, or break alerting
* Manual approval ensures a human reviews the changes before they hit production
* Staging deployment is automated (tag-triggered) because staging is non-production

---

## 13. Orchestration Design: 13 Tasks in Deliberate Order

### The Main Pipeline Job (ID: 254763181466712)

```
Task 1:  Setup Unity Catalog         → Creates schemas, tables, volumes if not exists
Task 2:  API Ingestion                → Fetches trading, market, risk data from APIs
Task 3:  Kafka Streaming              → Starts/monitors streaming ingestion from MSK
Task 4:  Operational Layer Processing → Bronze: raw → operational tables
Task 5:  Standardized Layer Processing→ Silver: operational → standardized tables
Task 6:  Reporting Layer Aggregation  → Gold: standardized → reporting tables
Task 7:  Regulatory Reporting          → Gold: standardized → regulatory tables
Task 8:  Data Quality Checks           → Runs DQ monitors on key tables
Task 9:  Alert Checks                  → Runs check_and_send_alerts.py
Task 10: Portfolio Metrics             → Additional portfolio-level calculations
Task 11: Risk Calculations             → VaR, stress tests, risk summaries
Task 12: Basel III Reporting           → RWA, CAR, LCR calculations
Task 13: Risk Reporting                → Final risk report generation
```

**Why this specific order?**

```
Task 1 (Setup) → must run first because it creates the schemas/tables that all other tasks write to

Tasks 2-3 (Ingestion) → must run before processing because you cannot process data that has not been ingested

Task 4 (Operational) → must run before Standardized because Silver reads from Bronze

Task 5 (Standardized) → must run before both Reporting (Task 6) and Regulatory (Task 7) because Gold reads from Silver

Tasks 6-7 (Gold) → can run in parallel after Standardized completes — they have no dependency on each other

Task 8 (DQ Checks) → runs after all data processing to validate the final state of all tables

Task 9 (Alerts) → runs after DQ checks so it can include DQ failures in the alert notifications

Tasks 10-13 (Specialized) → run last because they depend on reporting tables (Task 6) being populated
```

**Why not just one big notebook?**

* **Independent failure handling**: If Task 7 (Regulatory) fails, Tasks 1-6 still succeed and their data is preserved. The job can be retried from Task 7 only.
* **Independent retry**: Each task can have its own retry policy — ingestion tasks retry 3 times, processing tasks retry once.
* **Parallelism**: Tasks 6 and 7 can run in parallel, reducing total pipeline duration.
* **Monitoring**: Each task's duration, status, and logs are visible separately in the Jobs UI.
* **Dependency graph**: The task dependencies create a DAG that is visible in the UI — you can see exactly what is waiting on what.

### Why Daily at 6 AM UTC

* **6 AM UTC** = 1 AM EST (US market close is 4 PM EST, settlement happens overnight)
* By 6 AM UTC, the previous trading day's data is fully settled and available
* The pipeline finishes before US market open (9:30 AM EST = 14:30 UTC), giving analysts fresh data before market hours
* **Daily** frequency matches the trading day cycle — intraday runs would process partial data and produce inconsistent aggregates

---

## 14. Deployment Strategy: Three Environments, One Pipeline

### The Three Environments

| Environment | Catalog | Compute | Schedule | Purpose |
|---|---|---|---|---|
| Dev | `financial_lakehouse_staging` (or dev subset) | Serverless, small | Manual trigger | Development and testing |
| Staging | `financial_lakehouse_staging` | Serverless, medium | Triggered by release tag | Pre-production validation |
| Prod | `financial_lakehouse` | Serverless, production | Daily 6 AM UTC | Live data and regulatory reports |

**Why three environments (not two)?**

* **Dev** is for engineers to break things — they can test new code, modify tables, run experiments without consequences
* **Staging** is for validation — it runs the exact same code as prod but against staging data. This catches environment-specific issues (schema drift, data volume differences)
* **Prod** is the real thing — it runs against live data and produces regulatory reports

*With only two environments (dev + prod), you would either skip validation (risky) or validate in dev (which has different data than prod).*

### Why Serverless for All Environments

* **No cluster management**: Engineers do not configure instance types, worker counts, or auto-scaling rules
* **Auto-scaling**: Serverless automatically scales from 0 to N compute nodes based on workload
* **Auto-terminate**: Serverless shuts down after 10 minutes of idle — you pay for compute, not for idle time
* **Cost**: For our workload (daily batch + hourly DQ + weekly maintenance), serverless is cheaper than always-on clusters

### DABs Deployment Targets

```yaml
# dev target: uses dev catalog, smaller compute, no schedule
targets:
  dev:
    variables:
      catalog: financial_lakehouse_staging
      compute_size: small

# staging target: uses staging catalog, medium compute, triggered by tag
targets:
  staging:
    variables:
      catalog: financial_lakehouse_staging
      compute_size: medium

# prod target: uses prod catalog, production compute, daily schedule
targets:
  prod:
    variables:
      catalog: financial_lakehouse
      compute_size: production
```

**Why the same catalog for dev and staging?**

* Dev and staging share the `financial_lakehouse_staging` catalog to reduce infrastructure overhead
* They use different schemas within the catalog (dev writes to `dev_*` schemas, staging to `staging_*`)
* This is acceptable because dev and staging are never used simultaneously by different teams

---

## 15. Maintenance & Lifecycle: Keeping Delta Tables Healthy

### The Weekly Maintenance Job

Every Sunday, a maintenance job runs `VACUUM` and `OPTIMIZE` on all 16 Delta tables.

**Why VACUUM?**

* Delta Lake uses copy-on-write — every update creates a new version of the file, leaving the old version behind (for time travel)
* These old versions accumulate and consume storage space
* `VACUUM` deletes files older than the retention threshold (default 7 days), reclaiming storage
* **Why weekly?** Running VACUUM daily would delete files that might be needed for time travel queries within the same week. Weekly is a balance between storage savings and time travel capability.

**Why OPTIMIZE?**

* Streaming ingestion and frequent updates create many small files (one per micro-batch)
* Small files are inefficient to read — Spark must open each file separately, causing I/O overhead
* `OPTIMIZE` compacts small files into larger ones (typically 1 GB each), improving read performance
* **Why weekly?** Compacting daily would be wasteful (the daily data volume is not large enough). Monthly would let too many small files accumulate.

**Why ZORDER?**

* `OPTIMIZE ZORDER BY (transaction_id)` physically sorts data by `transaction_id` within files
* This enables data skipping — when you query `WHERE transaction_id = 'TXN12345'`, Spark can skip entire files that do not contain that ID
* Z-ordering is applied to columns frequently used in WHERE clauses: `transaction_id`, `account_id`, `trade_date`

**Why 16 tables?**

All Delta tables in the `operational`, `standardized`, `reporting`, and `regulatory` schemas need maintenance. The `raw` schema volumes do not need VACUUM (they are not Delta tables). Log tables (`pipeline_logs`, `dlt_event_logs`) are included because they accumulate many small writes.

### Retention Policies

Defined in table comments (not yet automated):

| Schema | Table | Retention | Reason |
|---|---|---|---|
| operational | trading_systems | 2 years | Raw data, can be re-ingested if needed |
| operational | pipeline_logs | 90 days | Logs are not needed long-term |
| reporting | daily_trading_summary | 2 years | Historical analysis |
| regulatory | * (all) | 7 years | Regulatory requirement |

**Why 7 years for regulatory?**

* Basel III, SOX, and BCBS 239 require financial institutions to retain regulatory data for 7 years
* This is a legal requirement, not a storage optimization decision
* Automated archival (move to S3 Glacier after 7 years) is planned but not yet implemented

---

## 16. Monitoring & Alerting: The Observability Stack

### The Complete Monitoring Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Observability Stack                           │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 1: Pipeline Execution Logs                        │  │
│  │  operational.pipeline_logs (Delta table)                 │  │
│  │  → Dual-write from PipelineLogger class                  │  │
│  │  → Stores: timestamps, levels, step names, durations    │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                            │                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 2: DLT Event Logs                                 │  │
│  │  operational.dlt_event_logs (Delta table)                │  │
│  │  → Exported from DLT internal event log                  │  │
│  │  → Stores: flow progress, DQ metrics, errors            │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                            │                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 3: Unified Monitoring View                        │  │
│  │  reporting.unified_log_monitoring (view)                 │  │
│  │  → UNION of pipeline logs + DLT events                   │  │
│  │  → Single SQL entry point for all monitoring queries    │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                            │                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 4: Alert Views (5 views)                          │  │
│  │  reporting.alert_job_failures                           │  │
│  │  reporting.alert_data_freshness                          │  │
│  │  reporting.alert_pipeline_errors                        │  │
│  │  reporting.alert_dq_failures                            │  │
│  │  reporting.alert_missing_data                           │  │
│  │  → Each view detects a specific failure pattern          │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                            │                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 5: Alert Orchestration                            │  │
│  │  13_monitoring/check_and_send_alerts.py                 │  │
│  │  → Queries all 5 alert views                             │  │
│  │  → Formats and sends alerts via Slack + email            │  │
│  │  → Runs as a task in the main pipeline job (Task 9)     │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                            │                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 6: External Notification                          │  │
│  │  Slack: #meridian-alerts channel                         │  │
│  │  Email: ops team distribution list                       │  │
│  │  Credentials: meridian_alerts secret scope                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 7: DQ Monitors (system-managed)                  │  │
│  │  3 monitors running hourly:                              │  │
│  │  → trading_systems_clean: null checks, row count         │  │
│  │  → account_performance: freshness, value range          │  │
│  │  → risk_summary: completeness, statistical checks        │  │
│  │  → Results in system.information_schema tables          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Layer 8: Dashboard (Lakeview)                            │  │
│  │  Meridian Pipeline Health Monitor                        │  │
│  │  → Visual overview of pipeline health                   │  │
│  │  → Auto-refreshing widgets from alert views             │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Why 8 layers?**

Each layer serves a specific purpose and can be modified independently:

* **Layers 1-2** (log collection): What data to capture. Modify these when you need new log fields.
* **Layer 3** (unification): How to combine logs. Modify this when log sources change.
* **Layer 4** (detection): What constitutes an alert. Modify these when alert rules change.
* **Layer 5** (orchestration): How to send alerts. Modify this when notification channels change.
* **Layer 6** (delivery): Where alerts go. Modify credentials when webhook URLs change.
* **Layer 7** (system DQ): Automated quality checks. Modify these when DQ rules change.
* **Layer 8** (visualization): Human-readable monitoring. Modify this when the team needs new dashboards.

### Why the Pipeline Health Monitor Dashboard

The dashboard (`Meridian Pipeline Health Monitor`) is a Lakeview dashboard that visualizes:
* Job success/failure rates over time
* Pipeline step durations
* Data freshness by table
* Alert counts by type

**Why a dashboard (not just alerts)?**

* Alerts tell you when something is broken — the dashboard tells you if things are degrading
* Trend visibility: if step durations are gradually increasing, that is a capacity problem before it becomes a failure
* Onboarding: new team members can see pipeline health at a glance without writing SQL

---

## 17. Technical Decision Log

A consolidated record of every significant technical decision and its rationale:

| # | Decision | Alternatives Considered | Why This Choice |
|---|---|---|---|
| 1 | Domain-oriented schema names (operational, standardized, reporting, regulatory) | Bronze/Silver/Gold | Business-stakeholder-readable; maps to team ownership |
| 2 | Single catalog with multiple schemas | Three separate catalogs | Simpler joins, unified governance, lower overhead |
| 3 | Serverless compute for all workloads | Classic clusters with auto-scaling | No cluster management, auto-terminate, pay-per-use |
| 4 | DLT for Bronze/Silver/Gold pipelines | Standard notebook jobs | Declarative, auto-retry, DQ expectations, lineage |
| 5 | Standard notebook jobs for regulatory calculations | DLT for everything | Complex SQL (CTEs, window functions) not DLT-friendly |
| 6 | Auto Loader for all ingestion | Direct Kafka consumer / JDBC | Exactly-once, schema evolution, incremental file discovery |
| 7 | Dual-write logging (console + Delta) | Log files only | Queryable, joinable, alertable, retained after cluster termination |
| 8 | DLT expect_all_or_drop for Silver DQ | expect_all_or_fail | Does not block entire pipeline on one bad record |
| 9 | SCD Type 2 via Delta MERGE | Overwrite + history table | Atomic, optimized by Delta Lake, no race conditions |
| 10 | Parametric VaR (mean - z * std_dev) | Historical simulation, Monte Carlo | Simplest to implement, good starting point, fast to compute |
| 11 | GitHub Actions + DABs for CI/CD | Jenkins, Azure DevOps | Native to GitHub, no separate server, DABs CLI integration |
| 12 | Manual approval for prod deployment | Auto-deploy on merge | Production impacts live data and regulatory reports |
| 13 | Weekly VACUUM/OPTIMIZE | Daily or monthly | Balances storage savings with time travel capability |
| 14 | 3 environments (dev, staging, prod) | 2 environments | Staging catches environment-specific issues before prod |
| 15 | Unity Catalog (not Hive metastore) | Hive metastore + external governance tools | Unified: tags, masking, RLS, lineage, audit in one place |
| 16 | Dynamic views for row-level security | Delta row filters | Available before row filters GA; migration planned |
| 17 | 13-task job with dependencies | Single large notebook | Independent failure handling, retry, parallelism, monitoring |
| 18 | Daily 6 AM UTC schedule | Hourly or real-time | Matches trading day cycle; data settled by 6 AM UTC |
| 19 | Slack + email for alerts | Slack only or email only | Slack for engineers, email for non-technical stakeholders |
| 20 | Separate raw schema (volumes) from operational (tables) | Single raw zone | Volumes preserve original files; tables are structured data |

---

## 18. Lessons Learned & Known Limitations

### What Works Well

* **Domain-oriented naming** has reduced confusion in stakeholder conversations — compliance officers immediately understand `regulatory` schema
* **Dual-write logging** has been transformative — we can SQL-query pipeline health instead of grep-ing through log files
* **DLT expectations** catch data quality issues at the source, preventing bad data from propagating
* **Serverless compute** has eliminated cluster management overhead — engineers focus on code, not infrastructure
* **DABs deployment** ensures dev/staging/prod consistency — no "it works on my workspace" issues

### Known Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| **No multi-region DR** | If us-east-1 goes down, all pipelines stop | Planned: DR workspace in us-west-2 with S3 CRR (see AWS infrastructure doc) |
| **Single catalog (not isolated lakehouses)** | Analytics and compliance data share a catalog | Sufficient for now; can split into separate catalogs as firm grows |
| **Parametric VaR only** | Does not capture fat tails or skewness | Planned: historical simulation VaR for regulatory reporting |
| **No currency conversion** | Cannot aggregate across currencies | Planned: reference data join with FX rates in Silver layer |
| **Limited BI dashboards** | Only 1 monitoring dashboard exists | Planned: 3-5 business dashboards for portfolio, risk, executive teams |
| **No automated archival** | Retention policies defined but not enforced | Planned: S3 lifecycle policies + automated purge jobs |
| **Manual governance tagging** | Tags applied manually, not automated | Planned: Active Metadata Automation (AMA) for automatic tag assignment |
| **Mock data (not real APIs)** | API ingestion uses mock data generator | Production: replace mock_data_generator with real API calls |
| **No cost dashboards** | Cannot see daily spend by team/workload | Planned: CloudWatch cost dashboard + Databricks cost analysis |
| **No DMS/Glue/Lambda ingestion** | Only API + Kafka + Auto Loader implemented | Planned: see gap analysis and AWS infrastructure docs |

### What We Would Do Differently

1. **Start with Delta row filters (not dynamic views)**: Row filters are now GA and provide better performance and simpler syntax than dynamic views
2. **Use `MERGE INTO` with SCD Type 2 from day one**: We initially tried overwrite + history tables; Delta MERGE is much cleaner
3. **Build more dashboards earlier**: We focused on pipelines and data quality; dashboards for business users were an afterthought
4. **Implement cost monitoring from day one**: Serverless costs can accumulate quickly if not monitored; we added cost alerting late
5. **Use DABs from the start**: We initially managed jobs through the Databricks UI; migrating to DABs required re-creating all job definitions in YAML

---

**Document Status**: Complete  
**Last Updated**: September 24, 2026  
**Maintained By**: Meridian Financial Lakehouse Engineering Team  
**Review Cycle**: After each major release or quarterly  
**Related Documents**:  
* [architecture.md](architecture.md) — High-level architecture overview  
* [gap_analysis.md](gap_analysis.md) — Gap analysis vs. target design  
* [aws_infrastructure.md](aws_infrastructure.md) — AWS infrastructure provisioning guide  
* [setup_guide.md](setup_guide.md) — Initial setup instructions  
* [data_dictionary.md](data_dictionary.md) — Table and column definitions