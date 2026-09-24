# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Observability Queries
# MAGIC ## Track DBU usage, storage costs, and optimization opportunities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Coverage
# MAGIC * DBU usage by workload type
# MAGIC * Storage costs by catalog/schema
# MAGIC * Cost per pipeline/job
# MAGIC * Optimization recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily DBU Usage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBU usage trend (last 30 days)
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) as total_dbu,
# MAGIC   SUM(usage_quantity * list_price) as estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date DESC, total_dbu DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DBU Usage by Workload Type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Breakdown by job vs interactive vs SQL warehouse
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   CASE 
# MAGIC     WHEN sku_name LIKE '%JOB%' THEN 'Jobs'
# MAGIC     WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 'Interactive'
# MAGIC     WHEN sku_name LIKE '%SQL%' THEN 'SQL Warehouse'
# MAGIC     WHEN sku_name LIKE '%DLT%' THEN 'Delta Live Tables'
# MAGIC     ELSE 'Other'
# MAGIC   END as workload_type,
# MAGIC   SUM(usage_quantity) as total_dbu,
# MAGIC   SUM(usage_quantity * list_price) as estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY usage_date, workload_type
# MAGIC ORDER BY usage_date DESC, total_dbu DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Storage Costs by Catalog

# MAGIC %md
# MAGIC ## 4. Cost Per Pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate DBU cost per pipeline run
# MAGIC WITH pipeline_costs AS (
# MAGIC   SELECT 
# MAGIC     p.pipeline_name,
# MAGIC     DATE(p.start_time) as run_date,
# MAGIC     TIMESTAMPDIFF(MINUTE, p.start_time, p.end_time) as duration_minutes,
# MAGIC     -- Estimate: 0.5 DBU per minute for standard cluster
# MAGIC     (TIMESTAMPDIFF(MINUTE, p.start_time, p.end_time) * 0.5) as estimated_dbu
# MAGIC   FROM system.lakeflow.pipeline_events p
# MAGIC   WHERE p.event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC     AND p.status = 'COMPLETED'
# MAGIC )
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   COUNT(*) as total_runs,
# MAGIC   SUM(duration_minutes) as total_runtime_min,
# MAGIC   ROUND(SUM(estimated_dbu), 2) as total_dbu,
# MAGIC   ROUND(SUM(estimated_dbu) / COUNT(*), 2) as avg_dbu_per_run,
# MAGIC   ROUND(SUM(estimated_dbu) * 0.15, 2) as estimated_cost_usd
# MAGIC FROM pipeline_costs
# MAGIC GROUP BY pipeline_name
# MAGIC ORDER BY total_dbu DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost Optimization Opportunities

# COMMAND ----------

# Identify cost optimization opportunities
def find_cost_optimization_opportunities():
    """
    Analyze usage patterns to find savings opportunities
    """
    opportunities = []
    
    # Check for idle clusters
    idle_clusters = spark.sql("""
        SELECT cluster_id, cluster_name, state, 
               TIMESTAMPDIFF(HOUR, last_activity_time, CURRENT_TIMESTAMP()) as hours_idle
        FROM system.compute.clusters
        WHERE state = 'RUNNING' 
          AND TIMESTAMPDIFF(HOUR, last_activity_time, CURRENT_TIMESTAMP()) > 2
    """).collect()
    
    if idle_clusters:
        for cluster in idle_clusters:
            opportunities.append({
                'type': 'Idle Cluster',
                'resource': cluster['cluster_name'],
                'recommendation': f"Cluster idle for {cluster['hours_idle']} hours. Consider auto-termination.",
                'potential_savings_pct': 30
            })
    
    # Check for oversized clusters
    # Check for inefficient pipeline schedules
    
    print("💰 Cost Optimization Opportunities:\n")
    if opportunities:
        for opp in opportunities:
            print(f"   • {opp['type']}: {opp['resource']}")
            print(f"     {opp['recommendation']}")
            print(f"     Potential Savings: {opp['potential_savings_pct']}%\n")
    else:
        print("   ✅ No major optimization opportunities found")
    
    return opportunities

opportunities = find_cost_optimization_opportunities()

# COMMAND ----------

print("✅ Cost observability queries complete")
