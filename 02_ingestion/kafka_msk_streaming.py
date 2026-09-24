# Databricks notebook source
# DBTITLE 1,Kafka/MSK Streaming Ingestion
# MAGIC %md
# MAGIC # Kafka/MSK Streaming Ingestion
# MAGIC ## Near real-time ingestion from AWS MSK (Managed Streaming for Kafka)
# MAGIC
# MAGIC This notebook implements streaming ingestion from Kafka topics for financial data sources:
# MAGIC * Trading systems events
# MAGIC * Market data feeds  
# MAGIC * Custody transaction streams

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define Kafka/MSK connection parameters, topics, and security settings

# COMMAND ----------

# DBTITLE 1,Kafka Configuration
# Kafka/MSK Configuration
# Replace with your actual MSK cluster endpoint
kafka_bootstrap_servers = dbutils.secrets.get(scope="meridian_lakehouse", key="kafka_bootstrap_servers")

# Topics to subscribe to
kafka_topics = [
    "trading-systems-events",
    "market-data-feed",
    "custody-transactions"
]

# Security Configuration for AWS MSK with IAM auth
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = dbutils.secrets.get(scope="meridian_lakehouse", key="kafka_sasl_mechanism")

# Checkpoint locations for each stream
checkpoint_base = "/Volumes/financial_lakehouse/raw/checkpoints/kafka"

# COMMAND ----------

# DBTITLE 1,Define Schema
# MAGIC %md
# MAGIC ## Trading Events Schema
# MAGIC Explicit schema for better performance and data quality validation

# COMMAND ----------

# DBTITLE 1,Trading Event Schema
trading_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("trade_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("quantity", DecimalType(18, 4), False),
    StructField("price", DecimalType(18, 4), False),
    StructField("trade_type", StringType(), False),  # BUY, SELL
    StructField("currency", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("broker", StringType(), True),
    StructField("settlement_date", DateType(), True),
    StructField("status", StringType(), False)  # PENDING, EXECUTED, SETTLED, CANCELLED
])

# COMMAND ----------

# DBTITLE 1,Read from Kafka
# MAGIC %md
# MAGIC ## Read Streaming Data from Kafka
# MAGIC Connect to Kafka and read messages with proper configuration for reliability

# COMMAND ----------

# DBTITLE 1,Kafka Stream Reader
# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", ",".join(kafka_topics)) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()

print("✓ Kafka stream initialized successfully")

# COMMAND ----------

# DBTITLE 1,Parse Messages
# MAGIC %md
# MAGIC ## Parse and Transform Kafka Messages
# MAGIC Extract JSON payloads and add metadata columns

# COMMAND ----------

# DBTITLE 1,Message Parser
# Parse Kafka messages and extract data
parsed_df = kafka_df \
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), trading_event_schema).alias("data")
    ) \
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("message_key"),
        col("data.*")
    ) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("kafka_msk")) \
    .withColumn("processing_date", current_date())

print("✓ Message parsing configured")

# COMMAND ----------

# DBTITLE 1,Write to Delta
# MAGIC %md
# MAGIC ## Write to Bronze Delta Table
# MAGIC Stream data to bronze layer with exactly-once semantics and checkpointing

# COMMAND ----------

# DBTITLE 1,Delta Writer
# Target table configuration
target_table = "financial_lakehouse.operational.trading_events_stream"
checkpoint_path = f"{checkpoint_base}/trading_events"

# Write to Delta with streaming
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(processingTime="30 seconds") \
    .toTable(target_table)

print(f"✓ Streaming to: {target_table}")
print(f"✓ Checkpoint: {checkpoint_path}")
print(f"✓ Trigger: Every 30 seconds")

# COMMAND ----------

# DBTITLE 1,Monitor Stream
# MAGIC %md
# MAGIC ## Monitor Stream Health
# MAGIC Check streaming query status, metrics, and processing rates

# COMMAND ----------

# DBTITLE 1,Stream Monitor
# Display stream status and metrics
try:
    print("\n" + "="*60)
    print("ACTIVE STREAMING QUERIES")
    print("="*60)
    
    for stream in spark.streams.active:
        print(f"\nStream ID: {stream.id}")
        print(f"Name: {stream.name if stream.name else 'Unnamed'}")
        print(f"Status: {stream.status}")
        
        if stream.recentProgress:
            latest = stream.recentProgress[-1]
            print(f"\nLatest Batch Metrics:")
            print(f"  Batch ID: {latest.get('batchId', 'N/A')}")
            print(f"  Input Rows: {latest.get('numInputRows', 0):,}")
            print(f"  Processing Rate: {latest.get('processedRowsPerSecond', 0):.2f} rows/sec")
            print(f"  Batch Duration: {latest.get('batchDuration', 0):.2f} ms")
        print("-" * 60)
        
except Exception as e:
    print(f"⚠ Stream monitoring error: {e}")

# COMMAND ----------

# DBTITLE 1,Cleanup
# MAGIC %md
# MAGIC ## Stop Streaming (Optional)
# MAGIC Uncomment to stop all active streams when needed

# COMMAND ----------

# DBTITLE 1,Stop Streams
# Uncomment to stop all streams
# for stream in spark.streams.active:
#     print(f"Stopping stream: {stream.id}")
#     stream.stop()
#     print(f"✓ Stream stopped")