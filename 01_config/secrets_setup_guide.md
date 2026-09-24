# Secrets Setup Guide

## Secret Scopes Created

| Scope | Purpose |
| --- | --- |
| `meridian_lakehouse` | API keys, Kafka credentials, pipeline secrets |
| `meridian_alerts` | Slack webhooks, alert email recipients |

## Required Secrets

### meridian_lakehouse

| Key | Description | Example |
| --- | --- | --- |
| `api_key_trading` | API key for trading systems endpoint | `sk-trading-xxxxx` |
| `api_key_market_data` | API key for market data endpoint | `sk-market-xxxxx` |
| `api_key_risk` | API key for risk metrics endpoint | `sk-risk-xxxxx` |
| `kafka_bootstrap_servers` | MSK cluster endpoint | `b-1.cluster.xxx.kafka.amazonaws.com:9092` |
| `kafka_sasl_mechanism` | Kafka auth mechanism | `AWS_MSK_IAM` |

### meridian_alerts

| Key | Description | Example |
| --- | --- | --- |
| `slack_webhook_url` | Slack incoming webhook URL for alert channel | `https://hooks.slack.com/services/xxx` |
| `alert_email` | Email address for alert notifications | `data-eng-team@company.com` |

## How to Add Secrets

1. Go to **Settings > Secrets** in Databricks workspace
2. Select scope `meridian_lakehouse` or `meridian_alerts`
3. Click **Add Secret**
4. Enter the key name and value
5. Repeat for each secret above

## Code Usage

Secrets are accessed in code via:
```python
api_key = dbutils.secrets.get(scope='meridian_lakehouse', key='api_key_trading')
slack_url = dbutils.secrets.get(scope='meridian_alerts', key='slack_webhook_url')
```

## CI/CD Secrets (GitHub)

Add these as repository secrets in GitHub Settings:
- `DATABRICKS_HOST` - workspace URL
- `DATABRICKS_TOKEN` - service principal PAT token
