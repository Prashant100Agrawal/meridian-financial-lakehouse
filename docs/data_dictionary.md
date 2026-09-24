# Data Dictionary

## Trading Systems Tables

### Bronze: trading_systems
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | string | Unique transaction identifier |
| account_id | string | Trading account ID |
| instrument_id | string | Instrument symbol |
| transaction_type | string | BUY or SELL |
| quantity | decimal(18,4) | Trade quantity |
| price | decimal(18,4) | Execution price |
| transaction_timestamp | timestamp | Trade execution time |
| currency | string | Currency code |
| trader_id | string | Trader identifier |
| ingestion_timestamp | timestamp | Data ingestion time |

### Silver: trading_systems_conformed
| Column | Type | Description |
|--------|------|-------------|
| txn_id | string | Transaction ID (conformed) |
| acct_id | string | Account ID (conformed) |
| symbol | string | Instrument symbol |
| side | string | BUY or SELL |
| qty | decimal(18,4) | Quantity |
| unit_price | decimal(18,4) | Price per unit |
| total_value | decimal(18,4) | Total trade value |
| trade_dt | timestamp | Trade date-time |
| ccy | string | Currency |
| trader_id | string | Trader ID |
| processed_dt | timestamp | Processing timestamp |

### Gold: daily_trading_summary
| Column | Type | Description |
|--------|------|-------------|
| trade_date | date | Trading date |
| symbol | string | Instrument symbol |
| side | string | BUY or SELL |
| num_trades | long | Number of trades |
| total_quantity | decimal(18,4) | Total quantity traded |
| total_value | decimal(18,4) | Total dollar value |
| avg_price | decimal(18,4) | Average price |
| min_price | decimal(18,4) | Minimum price |
| max_price | decimal(18,4) | Maximum price |
| created_timestamp | timestamp | Record creation time |
