# AWS Infrastructure Provisioning Guide

**Project**: Meridian Financial Lakehouse  
**Document Type**: Complete AWS Infrastructure Setup & Team Operations Guide  
**Date**: September 24, 2026  
**Audience**: DevOps / Platform Engineering / Data Engineering Leadership  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [AWS Account Strategy & Organization](#2-aws-account-strategy--organization)
3. [Network Architecture (VPC)](#3-network-architecture-vpc)
4. [Storage Architecture (S3)](#4-storage-architecture-s3)
5. [Databricks on AWS Deployment](#5-databricks-on-aws-deployment)
6. [Data Ingestion Services](#6-data-ingestion-services)
7. [Security & Compliance](#7-security--compliance)
8. [Compute & Processing](#8-compute--processing)
9. [CI/CD & Developer Tooling](#9-cicd--developer-tooling)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [High Availability & Disaster Recovery](#11-high-availability--disaster-recovery)
12. [Complete AWS Service Inventory](#12-complete-aws-service-inventory)
13. [Team Structure: 20 Data Engineers](#13-team-structure-20-data-engineers)
14. [Cost Estimation](#14-cost-estimation)
15. [Provisioning Roadmap](#15-provisioning-roadmap)
16. [Terraform IaC Structure](#16-terraform-iac-structure)

---

## 1. Executive Summary

This document describes the **complete AWS infrastructure** required to run the Meridian Financial Lakehouse at production scale with a team of **20 data engineers**. The architecture leverages **Databricks on AWS** as the core analytics platform, supplemented by native AWS services for data ingestion, security, networking, and observability.

### Design Principles

* **Security-first**: All data encrypted at rest and in transit; least-privilege IAM throughout
* **Multi-AZ by default**: Every service provisioned across at least 2 availability zones
* **Infrastructure as Code**: All resources provisioned via Terraform; no manual console changes
* **Environment isolation**: Separate dev, staging, and prod environments with independent resources
* **Cost-conscious**: Serverless and auto-scaling wherever possible; no idle resources
* **GitOps workflow**: All code and infrastructure changes flow through pull requests

### High-Level Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │            AWS Cloud (us-east-1)             │
                        │                                             │
   External Data ──────│──> API Gateway / S3 / MSK (Kafka)           │
   Sources              │         │                                   │
                        │         v                                   │
                        │   ┌─────────────────────────────────┐       │
                        │   │     VPC (10.0.0.0/16)           │       │
                        │   │  ┌───────────────────────┐     │       │
                        │   │  │  Private Subnets       │     │       │
                        │   │  │  (Databricks Data      │     │       │
                        │   │  │   Plane - clusters)   │     │       │
                        │   │  └───────────────────────┘     │       │
                        │   │  ┌───────────────────────┐     │       │
                        │   │  │  Public Subnets        │     │       │
                        │   │  │  (NAT Gateway, ALB)    │     │       │
                        │   │  └───────────────────────┘     │       │
                        │   └─────────────────────────────────┘       │
                        │         │                                   │
                        │         v                                   │
                        │   ┌─────────────────────────────────┐       │
                        │   │   S3 (Data Lake Storage)          │       │
                        │   │   - Raw landing                    │       │
                        │   │   - Checkpoints                    │       │
                        │   │   - Delta tables                  │       │
                        │   │   - Logs & artifacts               │       │
                        │   └─────────────────────────────────┘       │
                        │         │                                   │
                        │         v                                   │
                        │   ┌─────────────────────────────────┐       │
                        │   │   Databricks Workspace           │       │
                        │   │   (Control Plane - AWS managed)  │       │
                        │   │   - Serverless compute            │       │
                        │   │   - SQL Warehouses                │       │
                        │   │   - Unity Catalog                 │       │
                        │   └─────────────────────────────────┘       │
                        └─────────────────────────────────────────────┘
```

---

## 2. AWS Account Strategy & Organization

### Multi-Account Layout

Use AWS Organizations with a **hub-and-spoke** model to isolate environments and control blast radius:

```
AWS Organization
├── management-account (Root)
│   └── AWS Organizations, CloudTrail (org trail), AWS Config
│
├── security-account
│   └── AWS IAM Identity Center (SSO), GuardDuty, Security Hub,
│      AWS Audit Manager, KMS key store
│
├── logging-account
│   └── Centralized CloudWatch Logs, S3 access logs, Config logs,
│      CloudTrail S3 bucket (organization trail)
│
├── shared-services-account
│   └── Transit Gateway, VPC endpoints, Route 53 hosted zones,
│      Shared KMS keys, Secrets Manager
│
├── dev-account
│   └── Databricks dev workspace, S3 dev buckets, VPC dev,
│      All dev resources (non-production)
│
├── staging-account
│   └── Databricks staging workspace, S3 staging buckets, VPC staging,
│      Pre-prod validation environment
│
└── prod-account
    └── Databricks production workspace, S3 prod buckets, VPC prod,
       Production data, regulatory pipelines, live alerting
```

### IAM Identity Center (SSO)

All 20 data engineers authenticate via **AWS IAM Identity Center** (formerly AWS SSO), federated with the corporate IdP (Okta, Azure AD, or Google Workspace). No long-lived IAM access keys for individuals.

**Permission sets**:

| Permission Set | Description | Accounts | Who Gets It |
|---|---|---|---|
| `DataEngineer-Dev` | Full Databricks + S3 read/write in dev | dev | All 20 engineers |
| `DataEngineer-Staging` | Read-only Databricks + S3 read in staging | staging | Senior engineers (8) |
| `DataEngineer-Prod` | Read-only Databricks + S3 read-only in prod | prod | Tech leads (4) only |
| `PlatformAdmin` | Full VPC, IAM, KMS, S3 admin | shared, security, logging | DevOps / Platform team (2) |
| `SecurityAuditor` | Read-only across all accounts | all | Security officer (1) |

---

## 3. Network Architecture (VPC)

### VPC Design (Per Environment)

Each environment (dev, staging, prod) gets its own VPC in the same region (`us-east-1`):

```
VPC: 10.0.0.0/16
├── Public Subnets (10.0.0.0/24, 10.0.1.0/24)
│   ├── NAT Gateway (AZ-A)
│   ├── NAT Gateway (AZ-B)
│   └── Application Load Balancer (for API Gateway integrations)
│
├── Private Subnets - Databricks (10.0.10.0/23, 10.0.12.0/23)
│   ├── Databricks compute clusters (Spark driver + workers)
│   ├── SQL Warehouse compute
│   └── Serverless compute endpoints
│
├── Private Subnets - Data Services (10.0.20.0/23, 10.0.22.0/23)
│   ├── AWS MSK (Kafka) brokers
│   ├── AWS DMS replication instances
│   ├── AWS Glue Elastic Views (if used)
│   └── Lambda functions (VPC-attached)
│
├── Private Subnets - Databases (10.0.30.0/24, 10.0.31.0/24)
│   └── Source databases (RDS for DMS CDC sources, if applicable)
│
└── VPC Endpoints (Gateway + Interface)
    ├── s3 (Gateway endpoint)
    ├── dynamodb (Gateway endpoint, if used)
    ├── kms (Interface endpoint)
    ├── secretsmanager (Interface endpoint)
    ├── sts (Interface endpoint)
    ├── logs (Interface endpoint - CloudWatch)
    └── monitoring (Interface endpoint - CloudWatch metrics)
```

### Key Networking Components

| Component | Purpose | Configuration |
|---|---|---|
| **VPC** | Isolated network per environment | /16 CIDR, DNS hostnames enabled, DNS resolution enabled |
| **Public Subnets** | NAT, ALB, bastion | 2 subnets across 2 AZs (/24 each) |
| **Private Subnets (Databricks)** | All Databricks compute | 2 subnets across 2 AZs (/23 each) |
| **Private Subnets (Data Services)** | MSK, DMS, Lambda | 2 subnets across 2 AZs (/23 each) |
| **NAT Gateways** | Outbound internet for private subnets | 2 (one per AZ) for HA |
| **Internet Gateway** | Inbound/outbound for public subnets | 1 per VPC |
| **VPC Endpoints** | Private AWS API access (no NAT needed) | S3 gateway + interface endpoints |
| **Security Groups** | Instance-level firewall | See Security section |
| **Route 53 Private Hosted Zone** | Internal DNS resolution | For service discovery |
| **Transit Gateway** | Cross-account VPC peering | Connect dev/staging/prod to shared services |

### Security Groups

```
sg-databricks-compute
  ├── Inbound: All traffic from sg-databricks-compute (self-referencing)
  │           TCP 443 from sg-databricks-control (Databricks managed)
  ├── Outbound: All traffic to 0.0.0.0/0 (via NAT for package installs,
  │             PyPI, Maven) OR VPC endpoints for AWS services
  └── Tags: Environment=prod, Service=databricks

sg-msk-brokers
  ├── Inbound: TCP 9094 (Kafka TLS) from sg-databricks-compute
  │           TCP 9094 from sg-lambda-ingestion
  ├── Outbound: All traffic to VPC CIDR
  └── Tags: Environment=prod, Service=kafka

sg-dms-replication
  ├── Inbound: TCP 5432/3306 from source DB security group
  ├── Outbound: TCP 443 to S3 VPC endpoint
  │           TCP 9094 to sg-msk-brokers (if streaming to Kafka)
  └── Tags: Environment=prod, Service=dms

sg-lambda-ingestion
  ├── Inbound: TCP 443 from AWS API Gateway
  ├── Outbound: TCP 443 to S3 VPC endpoint
  │           TCP 9094 to sg-msk-brokers
  └── Tags: Environment=prod, Service=lambda
```

---

## 4. Storage Architecture (S3)

### S3 Bucket Strategy

Each environment has a primary S3 bucket with organized prefixes. Databricks uses S3 as the underlying storage layer for Delta Lake tables.

```
s3://meridian-lakehouse-dev/    (dev environment)
s3://meridian-lakehouse-staging/ (staging environment)
s3://meridian-lakehouse-prod/    (production environment)

Bucket Structure (per bucket):
├── raw/                          # Raw landing zone (pre-processed)
│   ├── trading-systems/          # API ingestion outputs
│   ├── market-data/               # Market data feeds
│   ├── vendor-files/             # External vendor CSV/XML/Parquet
│   ├── erp-data/                  # DMS CDC extracts
│   └── custody-data/              # Custody platform files
│
├── delta/                        # Delta Lake managed tables
│   ├── operational/              # operational schema tables
│   ├── standardized/             # standardized schema tables
│   ├── reporting/                # reporting schema tables
│   └── regulatory/               # regulatory schema tables
│
├── checkpoints/                  # Auto Loader & streaming checkpoints
│   ├── kafka-streaming/
│   ├── auto-loader/
│   └── dms-cdc/
│
├── artifacts/                    # Pipeline artifacts
│   ├── libs/                     # Python wheels, JARs
│   ├── scripts/                  # Job scripts
│   └── configs/                  # Pipeline configs
│
├── logs/                         # Application & pipeline logs
│   ├── spark-events/
│   ├── dlt-events/
│   └── pipeline-runs/
│
├── monitoring/                   # Monitoring assets
│   ├── alert-configs/
│   ├── dashboard-exports/
│   └── dq-reports/
│
├── archive/                      # Long-term retention (S3 lifecycle)
│   └── expired-data/             # Auto-archived via lifecycle policy
│
└── temp/                         # Temporary working files (auto-deleted)
    └── (S3 lifecycle: delete after 7 days)
```

### S3 Bucket Configuration

| Setting | Value | Reason |
|---|---|---|
| Versioning | Enabled | Protect against accidental deletes; support Delta Lake |
| Default encryption | SSE-KMS (customer-managed key) | Encryption at rest |
| Block public access | All settings ON | No public buckets |
| Bucket policy | Deny non-TLS requests | Enforce encryption in transit |
| Lifecycle rule 1 | Transition `temp/` to expire after 7 days | Cost savings |
| Lifecycle rule 2 | Transition `archive/` to Glacier after 90 days | Long-term storage |
| Lifecycle rule 3 | Transition `logs/` to Intelligent-Tiering | Cost optimization |
| CORS | Disabled (no browser access needed) | Security |
| Requester pays | Disabled | Standard billing |
| Transfer acceleration | Enabled for `raw/` prefix | Fast multi-region uploads |

### KMS Keys

| Key Alias | Purpose | Key Policy |
|---|---|---|
| `alias/meridian-s3-prod` | S3 bucket encryption (prod) | Databricks role + admin only |
| `alias/meridian-databricks-prod` | Databricks workspace encryption | Databricks service role |
| `alias/meridian-msk-prod` | MSK cluster encryption | MSK service role |
| `alias/meridian-secrets-prod` | Secrets Manager encryption | Application roles only |
| `alias/meridian-dms-prod` | DMS replication encryption | DMS service role |

---

## 5. Databricks on AWS Deployment

### Architecture: Control Plane vs Data Plane

```
┌────────────────────────────────────────────────────────────┐
│              Databricks Control Plane                       │
│              (AWS-managed, Databricks VPC)                  │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Workspace │  │   API    │  │   UC     │  │  Job     │  │
│  │  UI/API  │  │  Gateway │  │ Metastore │  │ Scheduler│  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│                         │                                  │
│          (Secure VPC peering / PrivateLink)                │
│                         │                                  │
└─────────────────────────┼──────────────────────────────────┘
                          │
┌─────────────────────────┼──────────────────────────────────┐
│              Customer Data Plane (Your VPC)                 │
│                          │                                  │
│  ┌───────────────────────┴──────────────────────────┐     │
│  │                                                   │     │
│  │  Private Subnets (10.0.10.0/23, 10.0.12.0/23)    │     │
│  │                                                   │     │
│  │  ┌────────────┐  ┌────────────┐  ┌───────────┐  │     │
│  │  │   Spark    │  │   Server-  │  │    SQL    │  │     │
│  │  │  Clusters  │  │   less     │  │ Warehouse │  │     │
│  │  │ (driver +  │  │  Compute   │  │  Compute  │  │     │
│  │  │  workers)  │  │            │  │           │  │     │
│  │  └─────┬──────┘  └─────┬──────┘  └─────┬─────┘  │     │
│  │        │               │                │        │     │
│  └────────┼───────────────┼────────────────┼────────┘     │
│           │               │                │              │
│           v               v                v              │
│  ┌─────────────────────────────────────────────┐         │
│  │         S3 (Delta Lake Storage)               │         │
│  │    via VPC Gateway Endpoint (private)        │         │
│  └─────────────────────────────────────────────┘         │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Databricks Workspace Configuration

| Setting | Dev | Staging | Prod |
|---|---|---|---|
| Workspace name | meridian-dev | meridian-staging | meridian-prod |
| Region | us-east-1 | us-east-1 | us-east-1 |
| VPC | 10.0.0.0/16 (dev) | 10.1.0.0/16 (staging) | 10.2.0.0/16 (prod) |
| Private subnets | 2 (AZ-A, AZ-B) | 2 (AZ-A, AZ-B) | 3 (AZ-A, AZ-B, AZ-C) |
| Security group | sg-databricks-dev | sg-databricks-staging | sg-databricks-prod |
| KMS key | alias/meridian-databricks-dev | alias/meridian-databricks-staging | alias/meridian-databricks-prod |
| Unity Catalog | Shared metastore (dev catalog) | Shared metastore (staging catalog) | Shared metastore (prod catalog) |
| Public IP | Disabled | Disabled | Disabled |
| Private DNS | Enabled | Enabled | Enabled |

### Unity Catalog Metastore

A single Unity Catalog metastore per environment, shared across all Databricks workspaces in that environment:

```
Unity Catalog Metastore (prod)
├── financial_lakehouse (catalog)
│   ├── raw (schema)
│   ├── operational (schema)
│   ├── standardized (schema)
│   ├── reporting (schema)
│   └── regulatory (schema)
├── financial_lakehouse_staging (catalog)
│   └── (same 5 schemas for pre-prod validation)
└── system (catalog)
    └── (Databricks-managed system tables)
```

### Databricks Storage Credentials

Unity Catalog uses storage credentials to access S3. These are backed by IAM roles:

| Storage Credential | IAM Role | Access |
|---|---|---|
| `meridian-prod-cred` | `role/meridian-databricks-prod-s3` | s3://meridian-lakehouse-prod/* |
| `meridian-staging-cred` | `role/meridian-databricks-staging-s3` | s3://meridian-lakehouse-staging/* |
| `meridian-dev-cred` | `role/meridian-databricks-dev-s3` | s3://meridian-lakehouse-dev/* |

---

## 6. Data Ingestion Services

### Ingestion Architecture Overview

```
Data Sources                AWS Ingestion Services          →  Databricks
─────────────              ──────────────────────             ──────────

Trading Systems (API)  ──>  API Gateway + Lambda          ──>  Auto Loader → S3
                         (REST ingestion, retry logic)

Market Data (Streaming)──>  AWS MSK (Kafka)               ──>  Structured Streaming
                         (6-broker cluster, TLS)

ERP Database (CDC)     ──>  AWS DMS                        ──>  S3 → Auto Loader
                         (replication instance, CDC)

Vendor Files (Batch)   ──>  S3 + S3 Event Notification    ──>  Auto Loader
                         (Lambda trigger validation)

Custody Data (Files)   ──>  S3 Upload + Lambda validation ──>  Auto Loader
                         (event-driven, schema validation)

Controlled Uploads     ──>  API Gateway + S3 Presigned   ──>  Auto Loader
                         (user-initiated, audited)
```

### 6.1 AWS MSK (Managed Streaming for Kafka)

**Purpose**: Real-time streaming ingestion for market data and trade events.

| Configuration | Value |
|---|---|
| Cluster type | Provisioned (for production) / Serverless (for dev) |
| Brokers | 6 (3 per AZ across 2 AZs) |
| Broker instance type | kafka.m5.large (prod), kafka.t3.small (dev) |
| Storage | EBS 100 GB per broker, auto-expansion enabled |
| Encryption | In-transit: TLS; At-rest: SSE-KMS |
| Authentication | IAM access control (no SASL/SCRAM) |
| Auto-scaling | Enabled (target: 65% CPU utilization) |
| Topics | `market-data-stream`, `trade-events`, `risk-metrics-stream` |
| Partition count | 12 per topic (parallelism for consumers) |
| Retention | 7 days (production), 24 hours (dev) |

**IAM roles**:

* `role/msk-producer`: Lambda functions and DMS that write to MSK
* `role/msk-consumer`: Databricks service role that reads from MSK (via Structured Streaming)

### 6.2 AWS DMS (Database Migration Service)

**Purpose**: CDC (Change Data Capture) from ERP and custody databases.

| Configuration | Value |
|---|---|
| Replication instance class | dms.r5.large (prod), dms.t3.medium (dev) |
| Replication instance count | 1 per source database |
| Multi-AZ | Yes (prod) |
| VPC | Private subnets (data services) |
| Migration type | CDC (ongoing replication) |
| Target | S3 in Parquet format (with partitioning by date) |
| CDC latency | Near real-time (< 5 minutes) |
| Encryption | SSE-KMS for S3 target, TLS for source connection |

**Endpoints**:

| Endpoint | Type | Engine | Details |
|---|---|---|---|
| `erp-source-prod` | Source | PostgreSQL | RDS Aurora PostgreSQL, VPC private |
| `custody-source-prod` | Source | Oracle | On-prem via VPN/DX, port 1521 |
| `s3-target-prod` | Target | S3 | s3://meridian-lakehouse-prod/raw/erp-data/ |

### 6.3 AWS Glue

**Purpose**: Scheduled batch processing for vendor file ingestion and data catalog.

| Configuration | Value |
|---|---|
| Glue version | 4.0 (Spark 3.3) |
| Worker type | G.1X (prod), G.025X (dev) |
| Workers | 10 (prod), 2 (dev) |
| Max concurrent DPUs | 100 |
| Glue Data Catalog | Per-environment catalog, integrated with Databricks |
| Crawlers | Run every 6 hours on `s3://raw/vendor-files/` |
| Jobs | `vendor-file-processor` (daily), `data-quality-validator` (hourly) |

**Glue Jobs**:

| Job Name | Schedule | Description |
|---|---|---|
| `vendor-file-ingestion` | Daily 2 AM UTC | Process new vendor CSV/XML files, validate schema, write to S3 raw |
| `glue-crawler-raw` | Every 6 hours | Update Data Catalog with new S3 objects |
| `data-quality-validator` | Hourly | Run DQ checks on raw data before Databricks picks up |

### 6.4 AWS Lambda

**Purpose**: Event-driven file validation and lightweight ingestion triggers.

| Function | Trigger | Runtime | Memory | Timeout | Purpose |
|---|---|---|---|---|---|
| `s3-file-validator` | S3 PUT (s3:ObjectCreated) on raw/vendor-files/ | Python 3.11 | 512 MB | 60s | Validate file format, size, schema; move invalid to quarantine |
| `api-ingestion-handler` | API Gateway (REST) | Python 3.11 | 1 GB | 120s | Handle REST API ingestion requests, forward to S3/MSK |
| `ddl-trigger` | DMS event | Python 3.11 | 256 MB | 30s | Trigger Databricks job when new CDC data arrives |
| `alert-forwarder` | SNS topic | Python 3.11 | 256 MB | 30s | Forward Databricks alerts to Slack/email via SNS |
| `cost-anomaly-detector` | EventBridge (daily) | Python 3.11 | 256 MB | 60s | Check AWS Cost Explorer anomalies, send alerts |

**Lambda VPC config**: All Lambda functions attached to private subnets (data services) with VPC endpoints for S3 access. No direct internet access.

### 6.5 Amazon API Gateway

**Purpose**: REST API endpoint for controlled data uploads and external API ingestion.

| Configuration | Value |
|---|---|
| API type | REST API (regional) |
| Throttling | 1000 RPS (prod), 100 RPS (dev) |
| Burst | 2000 (prod), 200 (dev) |
| Authorization | IAM authorizer + API key |
| Custom domain | api.meridian-lakehouse.internal (Route 53) |
| TLS | ACM-managed certificate |
| Logging | CloudWatch Logs (full request/response) |
| WAF | AWS WAF with rate-limiting and IP allowlist rules |

### 6.6 Amazon SNS & SQS

| Service | Purpose |
|---|---|
| `sns-meridian-alerts-prod` | SNS topic for pipeline alerts (Lambda → Slack/email) |
| `sns-meridian-cost-alerts` | SNS topic for AWS cost anomaly alerts |
| `sqs-dlq-ingestion` | Dead-letter queue for failed Lambda invocations |
| `sqs-dlq-dms` | Dead-letter queue for DMS task failures |

---

## 7. Security & Compliance

### Security Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Security Layers                          │
│                                                             │
│  Layer 1: Network Security                                  │
│  ├── VPC isolation (no public IPs for data plane)            │
│  ├── Security groups (least privilege)                      │
│  ├── VPC endpoints (no NAT for AWS services)                 │
│  ├── WAF on API Gateway                                     │
│  └── GuardDuty (threat detection)                           │
│                                                             │
│  Layer 2: Identity & Access                                 │
│  ├── IAM Identity Center (SSO, no access keys)              │
│  ├── IAM roles for services (cross-account via STS)         │
│  ├── Databricks workspace access (SCIM provisioning)         │
│  └── Unity Catalog permissions (RBAC on tables/schemas)     │
│                                                             │
│  Layer 3: Data Protection                                   │
│  ├── KMS encryption (customer-managed keys)                  │
│  ├── S3 bucket policies (TLS-only, no public access)         │
│  ├── Secrets Manager (API keys, DB credentials)             │
│  ├── Databricks secret scopes (backed by AWS Secrets Mgr)   │
│  ├── Unity Catalog column masking + row-level security       │
│  └── TLS 1.2+ everywhere                                     │
│                                                             │
│  Layer 4: Audit & Compliance                                │
│  ├── CloudTrail (all API calls, org trail)                  │
│  ├── AWS Config (configuration compliance)                 │
│  ├── Security Hub (aggregated findings)                     │
│  ├── Databricks audit logs (workspace, UC, jobs)           │
│  └── AWS Audit Manager (financial compliance frameworks)   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### IAM Roles (Service-Level)

| Role Name | Trust Policy | Permissions | Used By |
|---|---|---|---|
| `role/databricks-prod-s3` | Databricks account ID | s3://meridian-lakehouse-prod/* (read/write), KMS decrypt | Databricks UC storage credential |
| `role/databricks-prod-msk` | Databricks account ID | kafka-cluster:Connect, ReadData, DescribeCluster | Databricks Structured Streaming |
| `role/lambda-ingestion-prod` | lambda.amazonaws.com | s3:PutObject (raw/), kms:Encrypt, logs:CreateLogGroup | Lambda ingestion functions |
| `role/dms-replication-prod` | dms.amazonaws.com | s3:PutObject, kms:Encrypt, secretsmanager:GetSecretValue | DMS replication instances |
| `role/glue-job-prod` | glue.amazonaws.com | s3:GetObject/PutObject (raw/), glue:StartJobRun | Glue ETL jobs |
| `role/msk-producer-prod` | lambda.amazonaws.com | kafka-cluster:WriteData, DescribeCluster | Lambda → MSK |
| `role/msk-consumer-prod` | Databricks account ID | kafka-cluster:ReadData, DescribeCluster | Databricks → MSK |
| `role/sns-publisher` | lambda.amazonaws.com | sns:Publish | Lambda alert forwarder |

### Secrets Management

```
AWS Secrets Manager (per environment)
├── meridian/api-keys          # External API keys (market data, regulatory feeds)
├── meridian/kafka-sasl        # Kafka SASL credentials (if using SCRAM)
├── meridian/db-erp             # ERP database credentials (for DMS)
├── meridian/db-custody          # Custody database credentials
├── meridian/slack-webhook       # Slack webhook URL for alerts
├── meridian/email-smtp          # SMTP credentials for email alerts
├── meridian/databricks-token    # Databricks PAT for CI/CD
└── meridian/vendor-credentials # Vendor API credentials

Databricks Secret Scopes (backed by Secrets Manager):
├── meridian_lakehouse          # API keys, Kafka, DB credentials
└── meridian_alerts             # Slack, email credentials
```

### Compliance Frameworks

| Framework | How It's Addressed |
|---|---|
| **SOC 2 Type II** | CloudTrail, Config, Security Hub, encryption everywhere |
| **GDPR** | Data retention policies, right-to-erasure via S3 lifecycle, UC column masking for PII |
| **BCBS 239** (Banking) | Lineage tracking via Unity Catalog, audit trail in regulatory tables |
| **Basel III** | Regulatory tier tables, stress test data lineage |
| **IFRS 9** | ECL calculation audit trail in ifrs9_ecl table |
| **PCI DSS** (if applicable) | No card data stored; WAF, encryption, network isolation |

---

## 8. Compute & Processing

### Databricks Compute Resources

#### Serverless Compute (Primary)

All Databricks workloads use serverless compute. No traditional clusters are provisioned.

| Workload | Compute Type | Configuration |
|---|---|---|
| Pipeline notebooks | Serverless | Auto-scaling, auto-terminate |
| Streaming (Kafka) | Serverless | Sustained for 24/7 streaming |
| Ad-hoc analysis | Serverless | Auto-terminate after 20 min idle |
| DQ monitors | Serverless | Scheduled by Databricks |
| Maintenance (VACUUM/OPTIMIZE) | Serverless | Weekly scheduled |

#### SQL Warehouses

| Warehouse | Size | Auto-stop | Purpose |
|---|---|---|---|
| `meridian-prod-sql` | XXSMALL (Serverless) | 10 min | Production BI queries, dashboards, alerts |
| `meridian-staging-sql` | XXSMALL (Serverless) | 10 min | Staging validation queries |
| `meridian-dev-sql` | XXSMALL (Serverless) | 10 min | Developer ad-hoc queries |

#### Compute Policy (for 20 engineers)

Databricks cluster policies restrict what engineers can provision:

| Policy | Allowed Compute | Max DBUs/hour | Who |
|---|---|---|---|
| `engineer-dev` | Serverless only, max 2 concurrent | 50 | All 20 engineers |
| `engineer-staging` | Serverless only, max 1 concurrent | 30 | Senior engineers (8) |
| `engineer-prod-read` | SQL Warehouse only (no notebooks) | 20 | All engineers (read-only) |
| `lead-prod-write` | Serverless, max 1 concurrent | 40 | Tech leads (4) |

### AWS Compute (Non-Databricks)

| Service | Instance/Config | Purpose |
|---|---|---|
| **MSK Brokers** | 6x kafka.m5.large | Kafka streaming (prod) |
| **DMS Replication** | 1x dms.r5.large (Multi-AZ) | CDC from ERP/custody DBs |
| **Glue Workers** | 10x G.1X | Batch vendor file processing |
| **Lambda** | 5 functions (128MB-1GB each) | Event-driven ingestion & alerts |

---

## 9. CI/CD & Developer Tooling

### CI/CD Architecture

```
Developer Workstation
      │
      │ git push (feature branch)
      v
┌─────────────┐        PR opened
│   GitHub    │─────────────────>  ┌──────────────┐
│  Repository │                    │ GitHub Actions│
│             │<───── status ─────│  CI Pipeline  │
└─────────────┘                    └──────┬───────┘
      │                                   │
      │  merge to main                     │ deploy
      v                                   v
┌─────────────┐                  ┌──────────────┐
│ main branch │                  │  Databricks   │
│  (trunk)    │                  │  DABs deploy  │
└─────────────┘                  └──────────────┘
                                         │
                              ┌──────────┼──────────┐
                              v          v          v
                         dev    staging    prod
```

### GitHub Actions Pipeline

| Job | Trigger | What It Does |
|---|---|---|
| `validate` | Every PR | Lint Python (flake8), check SQL syntax, validate DABs bundle, run unit tests |
| `deploy-dev` | PR merge to main | Deploy DABs to dev workspace, run integration tests |
| `deploy-staging` | Tag release (v*.*.*) | Deploy DABs to staging workspace, run smoke tests |
| `deploy-prod` | Manual approval after staging | Deploy DABs to prod workspace, verify job health |

### Declarative Automation Bundles (DABs)

The `databricks.yml` file defines 3 targets (dev, staging, prod) with:

* **2 Jobs**: Main pipeline (13 tasks, daily 6 AM UTC), Maintenance (weekly Sunday)
* **1 Pipeline**: Spark Declarative Pipeline for streaming ingestion
* **1 Alert task**: Alert checks (check_and_send_alerts.py)

### Required GitHub Secrets

| Secret Name | Purpose | Where Stored |
|---|---|---|
| `DATABRICKS_HOST_DEV` | Dev workspace URL | GitHub Actions secrets |
| `DATABRICKS_TOKEN_DEV` | Dev Databricks PAT | GitHub Actions secrets (rotated quarterly) |
| `DATABRICKS_HOST_STAGING` | Staging workspace URL | GitHub Actions secrets |
| `DATABRICKS_TOKEN_STAGING` | Staging Databricks PAT | GitHub Actions secrets |
| `DATABRICKS_HOST_PROD` | Prod workspace URL | GitHub Actions secrets |
| `DATABRICKS_TOKEN_PROD` | Prod Databricks PAT | GitHub Actions secrets |

### Branch Protection Rules

| Rule | Setting |
|---|---|
| Require PR | Minimum 1 reviewer (2 for prod-impact changes) |
| Require status checks | validate job must pass |
| Require branches up to date | Yes |
| Dismiss stale reviews | Yes |
| Restrict who can push to main | Tech leads only |
| Enforce admins | Yes |

---

## 10. Monitoring & Observability

### Monitoring Stack

```
┌──────────────────────────────────────────────────────┐
│                  Observability Stack                   │
│                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐ │
│  │ CloudWatch   │  │ Databricks   │  │ Security Hub │ │
│  │ Dashboards   │  │ Monitoring   │  │              │ │
│  │              │  │              │  │  Findings     │ │
│  │ - MSK metrics │  │ - Job health │  │  GuardDuty   │ │
│  │ - DMS metrics │  │ - DQ alerts   │  │  Config rules │ │
│  │ - S3 metrics  │  │ - Pipeline    │  │  IAM analyzer │ │
│  │ - Lambda      │  │   logs        │  │              │ │
│  │   metrics     │  │ - UC lineage  │  │              │ │
│  └──────┬────────┘  └──────┬────────┘  └──────┬───────┘ │
│         │                  │                  │         │
│         v                  v                  v         │
│  ┌──────────────────────────────────────────────────┐ │
│  │              SNS + Slack + Email                  │ │
│  │         (Unified alerting channel)                │ │
│  └──────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### CloudWatch Dashboards

| Dashboard | Widgets | Refresh |
|---|---|---|
| `Meridian-MSK` | Bytes in/out, CPU, partition count, consumer lag | 1 min |
| `Meridian-DMS` | Replication lag, task status, CPU, memory | 1 min |
| `Meridian-S3` | Bucket size, request count, 4xx/5xx errors | 5 min |
| `Meridian-Lambda` | Invocation count, duration, errors, throttles | 1 min |
| `Meridian-Cost` | Daily spend, per-service breakdown, anomaly detection | 1 hour |

### CloudWatch Alarms

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| `MSK-HighCPU` | CPU utilization > 80% | 5 min | SNS → Slack #meridian-alerts |
| `MSK-ConsumerLag` | Consumer lag > 10000 | 1 min | SNS → Slack + PagerDuty |
| `DMS-ReplicationLag` | CDC lag > 300s | 1 min | SNS → Slack + email ops team |
| `DMS-TaskFailed` | Task status = Failed | Immediate | SNS → Slack + PagerDuty |
| `Lambda-Errors` | Errors > 10 | 5 min | SNS → Slack |
| `S3-4xxErrors` | 4xx errors > 100 | 5 min | SNS → Slack |
| `Cost-Anomaly` | Cost > 120% of 30-day average | Daily | SNS → email finance + Slack |

### Databricks Monitoring (Existing)

* **Pipeline Logs**: `operational.pipeline_logs` (dual-write structured logging)
* **DLT Event Logs**: `operational.dlt_event_logs` (automated export)
* **Unified Monitoring View**: `reporting.unified_log_monitoring`
* **5 Alert Views**: Job failures, data freshness, pipeline errors, DQ failures, missing data
* **DQ Monitors**: 3 monitors (hourly schedule) on key tables
* **Alert Orchestration**: `check_and_send_alerts.py` (Slack + email delivery)

---

## 11. High Availability & Disaster Recovery

### HA Strategy

| Layer | HA Approach | RTO | RPO |
|---|---|---|---|
| **Databricks Control Plane** | AWS-managed, multi-AZ by default | N/A | N/A |
| **Databricks Data Plane** | Compute across 3 AZs (prod) | 5 min | 0 (stateless compute) |
| **S3 Storage** | 99.999999911% durability, multi-AZ | 0 | 0 |
| **MSK Kafka** | 6 brokers across 3 AZs, replication factor 3 | 15 min | 0 |
| **DMS** | Multi-AZ replication instance | 10 min | < 5 min (CDC lag) |
| **Lambda** | Multi-AZ by default | 0 | N/A |
| **API Gateway** | Regional, multi-AZ by default | 0 | N/A |
| **VPC** | 3 AZs (prod), 2 AZs (dev/staging) | N/A | N/A |

### Disaster Recovery Plan

```
Primary Region: us-east-1 (N. Virginia)
DR Region: us-west-2 (Oregon)  ── [Warm standby]

DR Architecture:
├── S3 Cross-Region Replication (CRR)
│   └── s3://meridian-lakehouse-prod → s3://meridian-lakehouse-prod-dr
│       (All prefixes replicated with 15-min SLA)
│
├── Databricks DR Workspace (us-west-2)
│   └── Pre-provisioned, scaled to 50% of prod
│       (Activated during failover)
│
├── MSK Cross-Region Mirror
│   └── MirrorMaker 2.0 replicates topics to DR MSK cluster
│
├── DMS Cross-Region
│   └── DMS can be re-pointed to DR S3 bucket if needed
│
├── Route 53 Health Checks
│   └── DNS failover from primary to DR workspace
│
└── RTO / RPO Targets
    ├── RTO: 1 hour (activate DR workspace, update DNS, verify jobs)
    └── RPO: 15 minutes (S3 CRR + MSK mirror lag)
```

### DR Runbook (Summary)

1. **Detect**: CloudWatch alarm + Databricks job failure + SNS alert
2. **Assess**: Tech lead evaluates scope (single service vs. full region outage)
3. **Activate DR**:
   * Update Route 53 DNS to point to DR workspace URL
   * Scale up DR Databricks compute to 100%
   * Verify S3 CRR is up to date (check replication status)
   * Start MSK MirrorMaker if not already running
4. **Verify**: Run health check jobs, validate key tables, confirm streaming is active
5. **Communicate**: Notify stakeholders via Slack + email
6. **Failback** (when primary is restored):
   * Reverse CRR from DR → primary
   * Re-point DNS back to primary
   * Verify data consistency

---

## 12. Complete AWS Service Inventory

### All AWS Services Required

| # | AWS Service | Purpose | Environment |
|---|---|---|---|
| 1 | **S3** | Data lake storage (Delta Lake tables, raw data, logs, artifacts) | dev, staging, prod |
| 2 | **VPC** | Network isolation for all resources | dev, staging, prod |
| 3 | **IAM** | Service roles, permission sets, access control | All accounts |
| 4 | **KMS** | Customer-managed encryption keys | All accounts |
| 5 | **Secrets Manager** | API keys, DB credentials, webhooks | dev, staging, prod |
| 6 | **MSK (Kafka)** | Real-time streaming ingestion | prod (6 brokers), dev (serverless) |
| 7 | **DMS** | CDC from source databases (ERP, custody) | prod, dev |
| 8 | **Glue** | Batch ETL, data catalog, crawlers | prod, dev |
| 9 | **Lambda** | Event-driven ingestion, file validation, alert forwarding | prod, dev |
| 10 | **API Gateway** | REST API for controlled uploads | prod |
| 11 | **SNS** | Alert notifications, event fan-out | prod, staging |
| 12 | **SQS** | Dead-letter queues, async decoupling | prod |
| 13 | **CloudWatch** | Dashboards, alarms, log groups | All accounts |
| 14 | **CloudTrail** | API audit logging (organization trail) | logging account |
| 15 | **AWS Config** | Configuration compliance, drift detection | All accounts |
| 16 | **GuardDuty** | Threat detection | All accounts |
| 17 | **Security Hub** | Aggregated security findings | security account |
| 18 | **AWS Organizations** | Multi-account management | management account |
| 19 | **IAM Identity Center** | SSO for all 20 engineers | security account |
| 20 | **Route 53** | DNS, health checks, DR failover | shared services |
| 21 | **Transit Gateway** | Cross-account VPC connectivity | shared services |
| 22 | **VPC Endpoints** | Private AWS API access (S3, KMS, Secrets) | dev, staging, prod |
| 23 | **NAT Gateway** | Outbound internet for private subnets | dev, staging, prod |
| 24 | **WAF** | API Gateway protection | prod |
| 25 | **ACM** | TLS certificate management | prod |
| 26 | **EventBridge** | Scheduled Lambda triggers, event routing | prod |
| 27 | **AWS Cost Explorer** | Cost analysis and anomaly detection | management account |
| 28 | **AWS Audit Manager** | Financial compliance assessments | security account |
| 29 | **CloudFormation / Terraform** | Infrastructure as Code | N/A (tooling) |
| 30 | **CloudFront** | (Optional) CDN for dashboard assets if external access needed | prod |

### Service-to-Team Ownership Map

```
Platform/DevOps Team (2 engineers)
├── VPC, Security Groups, VPC Endpoints, Transit Gateway
├── IAM roles, KMS keys, Secrets Manager
├── Route 53, ACM, WAF
├── CloudTrail, Config, GuardDuty, Security Hub
├── AWS Organizations, IAM Identity Center
├── Terraform IaC repository
└── CI/CD pipeline (GitHub Actions infra)

Data Ingestion Team (4 engineers)
├── AWS MSK (Kafka)
├── AWS DMS (CDC replication)
├── AWS Glue (batch processing)
├── Lambda (ingestion functions)
├── API Gateway
├── S3 raw/ prefix management
└── SNS/SQS for ingestion events

Data Engineering Team (8 engineers)
├── Databricks serverless compute
├── Delta Lake tables (operational, standardized, reporting)
├── Databricks SQL Warehouses
├── Auto Loader pipelines
├── DQ monitors
└── S3 delta/ prefix management

Risk & Regulatory Team (3 engineers)
├── Regulatory tier tables (Basel III, IFRS 9, VaR)
├── Stress test pipelines
├── Regulatory audit trail
├── Databricks regulatory jobs
└── Compliance reporting

Analytics & Visualization (2 engineers)
├── BI dashboards (Lakeview)
├── SQL warehouse queries
├── CloudWatch dashboards
├── Cost dashboards
└── Alert configurations

Tech Lead / Architect (1 engineer)
├── Architecture decisions
├── Code review approvals
├── Production deployments
├── DR planning
└── Cross-team coordination
```

---

## 13. Team Structure: 20 Data Engineers

### Team Composition

```
                    ┌─────────────────────┐
                    │   Engineering Lead   │
                    │   (1 person)         │
                    │   Architecture,      │
                    │   code review, DR    │
                    └──────────┬──────────┘
                               │
            ┌──────────┬───────┴────┬──────────┐
            │          │            │          │
   ┌────────v───┐ ┌────v─────┐ ┌───v──────┐ ┌─v────────┐
   │ Platform   │ │ Ingestion│ │ Data Eng │ │ Risk &   │
   │ /DevOps    │ │ Team     │ │ Core     │ │ Reg.     │
   │ (2 eng)    │ │ (4 eng)  │ │ (8 eng)  │ │ (3 eng)  │
   └────────────┘ └──────────┘ └──────────┘ └──────────┘
                                                ┌──────────┐
                                                │ Analytics│
                                                │ (2 eng)  │
                                                └──────────┘
```

### Role Breakdown

| # | Role | Count | Responsibilities |
|---|---|---|---|
| 1 | Engineering Lead / Architect | 1 | Architecture decisions, code review for prod, DR planning, cross-team coordination, production deployment approval |
| 2 | Platform / DevOps Engineer | 2 | AWS infrastructure (Terraform), VPC, IAM, KMS, CI/CD pipeline, monitoring infra, security compliance |
| 3 | Data Ingestion Engineer | 4 | MSK Kafka, DMS CDC, Glue jobs, Lambda functions, API Gateway, S3 raw ingestion, vendor file processing |
| 4 | Core Data Engineer | 8 | Databricks pipelines, Delta tables (operational/standardized/reporting), SQL warehouses, DQ monitors, Auto Loader |
| 5 | Risk & Regulatory Engineer | 3 | Basel III, IFRS 9, VaR, stress test pipelines, regulatory audit trail, compliance reporting |
| 6 | Analytics & Visualization Engineer | 2 | BI dashboards, Lakeview dashboards, cost dashboards, alert configuration, business user support |

### Developer Workflow

#### Daily Workflow for Each Engineer

```
1. Morning Standup (15 min)
   ├── Review overnight job status (Databricks job runs, CloudWatch alarms)
   ├── Check Slack #meridian-alerts for any failures
   └── Assign on-call rotation for incident response

2. Development (4-6 hours)
   ├── Pull latest from main branch
   ├── Create feature branch: feature/<ticket>-<description>
   ├── Develop in Databricks dev workspace (serverless compute)
   ├── Test locally against dev S3 bucket
   ├── Run unit tests + integration tests
   └── Commit and push to feature branch

3. Code Review (1-2 hours)
   ├── Open PR to main
   ├── CI pipeline runs (validate job: lint, test, DABs validate)
   ├── Request review from team lead or peer
   ├── Address review feedback
   └── Merge to main (auto-deploys to dev)

4. Deployment (coordinated)
   ├── Dev: Auto-deployed on merge to main
   ├── Staging: Tag release, auto-deploy + smoke tests
   └── Prod: Manual approval by tech lead, then deploy
```

#### Git Branching Strategy

```
main (trunk)
  │
  ├── feature/ingestion-api          # Ingestion team feature branch
  ├── feature/regulatory-basel3      # Risk team feature branch
  ├── feature/dq-monitoring          # Data eng feature branch
  ├── feature/cost-dashboard         # Analytics feature branch
  ├── feature/terraform-vpc-update   # Platform team infra branch
  │
  ├── release/v1.0                   # Staging release tag
  ├── release/v1.1                   # Next release tag
  │
  └── (hotfix/ prefix for urgent prod fixes)
```

**Rules**:
* No direct commits to `main`
* Feature branches auto-deleted after merge
* Hotfix branches require 2 reviewers
* Release tags trigger staging deployment
* Production deployment requires manual approval

#### Access Control Matrix

| Capability | Platform (2) | Ingestion (4) | Data Eng (8) | Risk (3) | Analytics (2) | Lead (1) |
|---|---|---|---|---|---|---|
| AWS Console (dev) | Full | Read | Read | Read | Read | Full |
| AWS Console (prod) | Read | None | None | None | None | Read |
| Databricks Dev | Admin | Contributor | Contributor | Contributor | Contributor | Admin |
| Databricks Staging | Admin | Read | Read | Read | Read | Admin |
| Databricks Prod | Admin | Read | Read | Read | Read | Admin |
| Merge to main | Yes | Yes | Yes | Yes | Yes | Yes |
| Deploy to prod | No | No | No | No | No | Yes |
| Terraform apply (prod) | Yes | No | No | No | No | Yes |
| Rotate secrets | Yes | No | No | No | No | Yes |
| Create IAM roles | Yes | No | No | No | No | Yes |

#### On-Call Rotation

```
Weekly rotation (7 days, Mon-Sun):

Week 1: Ingestion engineer (primary) + Platform engineer (secondary)
Week 2: Data eng engineer (primary) + Platform engineer (secondary)
Week 3: Risk engineer (primary) + Platform engineer (secondary)
Week 4: Analytics engineer (primary) + Platform engineer (secondary)
Week 5: Engineering lead (primary) + Platform engineer (secondary)

On-call responsibilities:
* Respond to PagerDuty/Slack alerts within 15 min (prod), 1 hr (non-prod)
* Triage CloudWatch alarms and Databricks job failures
* Escalate to tech lead for production-impact incidents
* Document incidents in postmortem (within 48 hours)
```

#### Communication Channels

| Channel | Purpose |
|---|---|
| `#meridian-engineering` | General engineering discussion |
| `#meridian-alerts` | Automated alerts (CI/CD, pipeline, DQ, cost) |
| `#meridian-incidents` | Incident response and coordination |
| `#meridian-deployments` | Deployment notifications |
| `#meridian-oncall` | On-call handoff and escalations |
| `#meridian-architecture` | Architecture decisions and RFCs |

---

## 14. Cost Estimation

### Monthly Cost Estimate (Production Environment)

| Service | Configuration | Est. Monthly Cost (USD) |
|---|---|---|
| Databricks Serverless Compute | ~1000 DBUs/month | $2,500 |
| Databricks SQL Warehouse (Serverless) | ~200 DBUs/month | $500 |
| S3 Storage | ~5 TB (Delta + raw + logs) | $115 |
| MSK (Kafka) | 6x m5.large brokers | $1,200 |
| DMS Replication | 1x r5.large (Multi-AZ) | $250 |
| Glue Jobs | ~100 DPU-hours/month | $150 |
| Lambda | ~10M invocations/month | $50 |
| API Gateway | ~10M requests/month | $35 |
| NAT Gateway | 2x (prod) | $90 |
| VPC Endpoints | ~8 interface endpoints | $30 |
| KMS Keys | 5 customer-managed keys | $25 |
| Secrets Manager | ~10 secrets | $4 |
| CloudWatch | Dashboards + logs + alarms | $100 |
| Route 53 | Hosted zones + health checks | $15 |
| GuardDuty | 1 account, all detectors | $50 |
| Security Hub | Standard checks | $10 |
| S3 Cross-Region Replication | ~2 TB to DR region | $50 |
| **Total (Production)** | | **~$5,200/month** |

### Dev + Staging Environments

| Environment | Est. Monthly Cost |
|---|---|
| Dev | ~$1,000 (smaller compute, less data) |
| Staging | ~$1,500 (mid-scale, periodic testing) |
| **Grand Total (all 3 environments)** | **~$7,700/month** |

### Cost Optimization Tips

* Use S3 Intelligent-Tiering for infrequently accessed data
* Set Databricks auto-terminate to 10 min for all non-streaming compute
* Use MSK Serverless for dev (pay-per-request, no idle brokers)
* Schedule Glue crawlers/jobs during off-peak hours
* Use CloudWatch Logs retention (90 days max for non-prod)
* Set up AWS Budgets alerts at 80% and 100% of monthly budget

---

## 15. Provisioning Roadmap

### Phase 1: Foundation (Weeks 1-2)

* [ ] Create AWS Organization with 7 accounts (management, security, logging, shared, dev, staging, prod)
* [ ] Set up IAM Identity Center with corporate IdP federation
* [ ] Provision VPCs in all 3 environment accounts (dev, staging, prod)
* [ ] Configure VPC endpoints, NAT gateways, security groups
* [ ] Create S3 buckets with KMS encryption and lifecycle policies
* [ ] Set up KMS keys for each environment
* [ ] Configure CloudTrail (org trail), AWS Config, GuardDuty
* [ ] Set up Route 53 hosted zones

### Phase 2: Databricks Deployment (Weeks 3-4)

* [ ] Deploy Databricks workspaces in dev, staging, prod accounts
* [ ] Configure VPC peering / PrivateLink between Databricks control plane and data plane
* [ ] Create Unity Catalog metastores (per environment)
* [ ] Create storage credentials and external locations
* [ ] Set up Databricks secret scopes backed by AWS Secrets Manager
* [ ] Configure SCIM user provisioning from IdP
* [ ] Deploy existing `databricks.yml` bundle to all 3 environments

### Phase 3: Data Ingestion (Weeks 5-8)

* [ ] Provision MSK Kafka cluster (prod: 6 brokers, dev: serverless)
* [ ] Create Kafka topics and configure IAM access control
* [ ] Set up DMS replication instances and endpoints (ERP, custody)
* [ ] Configure DMS CDC tasks with S3 target
* [ ] Deploy Glue jobs for vendor file processing
* [ ] Create Lambda functions for S3 file validation and API ingestion
* [ ] Set up API Gateway with WAF protection
* [ ] Configure SNS topics and SQS dead-letter queues
* [ ] Wire ingestion pipelines to Databricks Auto Loader

### Phase 4: Security & Compliance (Weeks 9-10)

* [ ] Configure all IAM roles and permission sets
* [ ] Set up Secrets Manager with all required secrets
* [ ] Enable Security Hub with all standard controls
* [ ] Configure AWS Config rules for compliance
* [ ] Set up AWS Audit Manager for financial compliance frameworks
* [ ] Implement Databricks workspace access controls
* [ ] Configure Unity Catalog governance (tags, masking, row-level security)

### Phase 5: Monitoring & DR (Weeks 11-12)

* [ ] Create CloudWatch dashboards (MSK, DMS, S3, Lambda, Cost)
* [ ] Configure all CloudWatch alarms with SNS integration
* [ ] Set up S3 Cross-Region Replication to DR region (us-west-2)
* [ ] Deploy DR Databricks workspace in us-west-2
* [ ] Configure MSK MirrorMaker 2.0 for cross-region replication
* [ ] Set up Route 53 health checks and DNS failover
* [ ] Document DR runbook and conduct failover test

### Phase 6: Team Onboarding (Weeks 13-14)

* [ ] Provision all 20 engineers in IAM Identity Center
* [ ] Assign permission sets per role
* [ ] Create Databricks workspace users via SCIM
* [ ] Configure Databricks cluster policies per team
* [ ] Set up Slack channels and alert routing
* [ ] Configure PagerDuty / on-call rotation
* [ ] Conduct training sessions on infrastructure and workflows
* [ ] Run first full-team sprint with CI/CD pipeline

---

## 16. Terraform IaC Structure

### Recommended Repository Structure

```
meridian-infrastructure/          (Separate Terraform repo)
├── modules/
│   ├── vpc/                       # VPC, subnets, NAT, IGW, route tables
│   ├── s3/                        # S3 buckets, policies, lifecycle
│   ├── kms/                       # KMS keys and aliases
│   ├── iam/                       # IAM roles, policies, permission sets
│   ├── msk/                       # MSK cluster, topics, IAM auth
│   ├── dms/                       # DMS replication instances, endpoints
│   ├── glue/                      # Glue jobs, crawlers, connections
│   ├── lambda/                    # Lambda functions, layers, triggers
│   ├── apigateway/                # API Gateway, routes, integrations
│   ├── monitoring/               # CloudWatch dashboards, alarms, SNS
│   ├── security/                  # GuardDuty, Security Hub, Config
│   ├── secrets/                   # Secrets Manager secrets
│   ├── networking/               # VPC endpoints, Transit Gateway
│   └── databricks/                # Databricks workspace, UC metastore
│
├── environments/
│   ├── dev/
│   │   ├── main.tf               # Module composition for dev
│   │   ├── variables.tf
│   │   ├── terraform.tfvars       # Dev-specific values
│   │   └── backend.tf             # S3 backend (state in logging account)
│   ├── staging/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── terraform.tfvars
│   │   └── backend.tf
│   └── prod/
│       ├── main.tf
│       ├── variables.tf
│       ├── terraform.tfvars
│       └── backend.tf
│
├── shared/
│   ├── organizations/             # AWS Org, accounts, SCPs
│   ├── identity-center/           # SSO permission sets, group mappings
│   ├── cloudtrail/                # Org trail
│   ├── transit-gateway/          # Cross-account networking
│   └── route53/                   # Shared DNS
│
├── policies/                      # IAM policy documents (JSON)
│   ├── databricks-s3.json
│   ├── databricks-msk.json
│   ├── lambda-ingestion.json
│   ├── dms-replication.json
│   └── glue-jobs.jsonn│
├── README.md
└── .github/
    └── workflows/
        └── terraform-ci.yml       # Terraform validate, plan, apply
```

### Terraform Backend

State is stored in the logging account's S3 bucket with DynamoDB locking:

```hcl
terraform {
  backend "s3" {
    bucket         = "meridian-terraform-state"
    key            = "environments/prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "meridian-terraform-locks"
    encrypt        = true
    kms_key_id     = "alias/meridian-terraform"
  }
}
```

### Key Terraform Modules (Examples)

#### VPC Module

```hcl
module "vpc" {
  source = "../../modules/vpc"

  environment           = "prod"
  cidr_block            = "10.2.0.0/16"
  availability_zones    = ["us-east-1a", "us-east-1b", "us-east-1c"]
  public_subnets        = ["10.2.0.0/24", "10.2.1.0/24", "10.2.2.0/24"]
  private_subnets      = ["10.2.10.0/23", "10.2.12.0/23", "10.2.14.0/23"]
  data_subnets          = ["10.2.20.0/23", "10.2.22.0/23", "10.2.24.0/23"]
  enable_nat_gateway    = true
  single_nat_gateway    = false  # Use one NAT per AZ for HA
  enable_vpc_endpoints  = true
}
```

#### MSK Module

```hcl
module "msk" {
  source = "../../modules/msk"

  environment          = "prod"
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.data_subnet_ids
  security_group_ids  = [aws_security_group.msk.id]
  broker_instance_type = "kafka.m5.large"
  broker_count         = 6
  kafka_version        = "3.5.1"
  encryption_at_rest   = true
  kms_key_id           = aws_kms_key.msk.arn
  enable_iam_auth      = true
  topics = {
    "market-data-stream"    = { partitions = 12, replicas = 3, retention = "168h" }
    "trade-events"          = { partitions = 12, replicas = 3, retention = "168h" }
    "risk-metrics-stream"   = { partitions = 6,  replicas = 3, retention = "168h" }
  }
}
```

#### DMS Module

```hcl
module "dms" {
  source = "../../modules/dms"

  environment          = "prod"
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.data_subnet_ids
  security_group_ids  = [aws_security_group.dms.id]
  replication_class   = "dms.r5.large"
  multi_az            = true
  kms_key_id          = aws_kms_key.dms.arn

  source_endpoints = {
    erp     = { engine = "postgres", server = "erp-db.internal", port = 5432 }
    custody = { engine = "oracle",  server = "custody-db.internal", port = 1521 }
  }
  target_endpoint = {
    engine   = "s3"
    s3_bucket = module.s3.bucket_id
    s3_prefix = "raw/"
  }
}
```

### Terraform CI Pipeline

```yaml
# .github/workflows/terraform-ci.yml
name: Terraform CI
on:
  pull_request:
    paths: ["**/*.tf"]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform fmt -check
      - run: terraform init
      - run: terraform validate
      - run: terraform plan -var-file=environments/dev/terraform.tfvars

  apply-staging:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: staging  # Requires manual approval
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init
      - run: terraform apply -auto-approve -var-file=environments/staging/terraform.tfvars

  apply-prod:
    needs: apply-staging
    runs-on: ubuntu-latest
    environment: production  # Requires manual approval
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init
      - run: terraform apply -auto-approve -var-file=environments/prod/terraform.tfvars
```

---

## Appendix A: AWS Region Selection

| Region | Use | Reason |
|---|---|---|
| us-east-1 (N. Virginia) | Primary | Most service availability, lowest cost, proximity to financial data centers |
| us-west-2 (Oregon) | DR | Geographically diverse, full service parity |

## Appendix B: Useful AWS CLI Commands for Verification

```bash
# Verify VPC and subnets
aws ec2 describe-vpcs --filters Name=tag:Environment,Values=prod
aws ec2 describe-subnets --filters Name=vpc-id,Values=vpc-xxx

# Verify MSK cluster
aws kafka describe-cluster --cluster-arn arn:aws:kafka:us-east-1:xxx:cluster/meridian-prod/xxx

# Verify DMS replication instances
aws dms describe-replication-instances

# Verify S3 bucket encryption
aws s3api get-bucket-encryption --bucket meridian-lakehouse-prod

# Verify KMS keys
aws kms list-aliases | grep meridian

# Verify IAM Identity Center users
aws sso list-users --instance-arn arn:aws:sso:::instance/ssoins-xxx

# Verify CloudTrail
aws cloudtrail describe-trails

# Verify VPC endpoints
aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values=vpc-xxx
```

## Appendix C: Environment Variable Reference

```bash
# Dev environment
export AWS_PROFILE=meridian-dev
export DATABRICKS_HOST=https://dbc-xxx-dev.cloud.databricks.com
export DATABRICKS_TOKEN=dbapi_xxx  # Use AWS Secrets Manager in prod
export S3_BUCKET=meridian-lakehouse-dev
export UC_METASTORE=arn:aws:databricks:us-east-1:xxx:metastore/meridian-dev

# Staging environment
export AWS_PROFILE=meridian-staging
export DATABRICKS_HOST=https://dbc-xxx-staging.cloud.databricks.com
export S3_BUCKET=meridian-lakehouse-staging

# Production environment
export AWS_PROFILE=meridian-prod
export DATABRICKS_HOST=https://dbc-xxx-prod.cloud.databricks.com
export S3_BUCKET=meridian-lakehouse-prod
```

---

**Document Status**: Complete  
**Last Updated**: September 24, 2026  
**Maintained By**: Meridian Financial Lakehouse Engineering Team  
**Review Cycle**: Quarterly or upon major architecture change