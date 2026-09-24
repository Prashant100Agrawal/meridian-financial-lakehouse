# Databricks notebook source
# MAGIC %md
# MAGIC # Dependency Management
# MAGIC ## Track and enforce pipeline and job dependencies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Features
# MAGIC * Explicit dependency DAG
# MAGIC * Pre-execution validation
# MAGIC * Dependency health checks
# MAGIC * Execution ordering

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Dependencies

# COMMAND ----------

pipeline_dependencies = {
    'bronze_trading_ingestion': {
        'upstream': [],  # No dependencies (source)
        'downstream': ['silver_trading_clean'],
        'sla_minutes': 30
    },
    'silver_trading_clean': {
        'upstream': ['bronze_trading_ingestion'],
        'downstream': ['gold_trading_summary', 'gold_account_performance'],
        'sla_minutes': 15
    },
    'gold_trading_summary': {
        'upstream': ['silver_trading_clean'],
        'downstream': ['analytics_dashboard'],
        'sla_minutes': 10
    },
    'gold_account_performance': {
        'upstream': ['silver_trading_clean'],
        'downstream': ['risk_reporting', 'compliance_reporting'],
        'sla_minutes': 15
    },
    'risk_reporting': {
        'upstream': ['gold_account_performance'],
        'downstream': [],
        'sla_minutes': 20
    }
}

print("📊 Pipeline Dependency Graph:")
for pipeline, deps in pipeline_dependencies.items():
    upstream = ', '.join(deps['upstream']) if deps['upstream'] else 'SOURCE'
    downstream = ', '.join(deps['downstream']) if deps['downstream'] else 'LEAF'
    print(f"\n   {pipeline}:")
    print(f"      ⬆️  Upstream: {upstream}")
    print(f"      ⬇️  Downstream: {downstream}")
    print(f"      ⏱️  SLA: {deps['sla_minutes']} min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dependency Health Check

# COMMAND ----------

def check_upstream_health(pipeline_name, lookback_hours=24):
    """
    Verify all upstream dependencies completed successfully
    
    Returns:
        dict: Health status of upstream dependencies
    """
    deps = pipeline_dependencies.get(pipeline_name, {})
    upstream = deps.get('upstream', [])
    
    if not upstream:
        return {'healthy': True, 'message': 'No upstream dependencies'}
    
    print(f"🔍 Checking upstream health for {pipeline_name}...")
    
    unhealthy = []
    for upstream_pipeline in upstream:
        # Check if upstream completed successfully recently
        query = f"""
            SELECT 
                pipeline_name,
                MAX(end_time) as last_success,
                TIMESTAMPDIFF(HOUR, MAX(end_time), CURRENT_TIMESTAMP()) as hours_ago
            FROM system.lakeflow.pipeline_events
            WHERE pipeline_name = '{upstream_pipeline}'
              AND status = 'COMPLETED'
              AND event_date >= CURRENT_DATE() - INTERVAL {lookback_hours} HOURS
            GROUP BY pipeline_name
        """
        
        result = spark.sql(query).collect()
        
        if not result:
            unhealthy.append(f"{upstream_pipeline}: No recent successful runs")
            print(f"   ❌ {upstream_pipeline}: No recent completion")
        else:
            hours_ago = result[0]['hours_ago']
            sla = pipeline_dependencies[upstream_pipeline]['sla_minutes'] / 60.0
            
            if hours_ago > sla:
                unhealthy.append(f"{upstream_pipeline}: Last run {hours_ago:.1f}h ago (SLA: {sla:.1f}h)")
                print(f"   ⚠️  {upstream_pipeline}: Stale ({hours_ago:.1f}h ago)")
            else:
                print(f"   ✅ {upstream_pipeline}: Healthy")
    
    if unhealthy:
        return {
            'healthy': False,
            'message': f"Upstream issues: {'; '.join(unhealthy)}"
        }
    
    return {'healthy': True, 'message': 'All upstream dependencies healthy'}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pre-Execution Validation

# COMMAND ----------

def validate_before_execution(pipeline_name):
    """
    Validate all preconditions before pipeline execution
    """
    print(f"\n{'='*80}")
    print(f"PRE-EXECUTION VALIDATION: {pipeline_name}")
    print('='*80)
    
    validations = []
    
    # 1. Check upstream dependencies
    upstream_health = check_upstream_health(pipeline_name)
    validations.append({
        'check': 'Upstream Dependencies',
        'passed': upstream_health['healthy'],
        'message': upstream_health['message']
    })
    
    # 2. Check cluster availability (if applicable)
    # 3. Check data freshness
    # 4. Check storage quota
    
    # Summary
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print('='*80)
    
    all_passed = all(v['passed'] for v in validations)
    
    for validation in validations:
        status = "✅ PASS" if validation['passed'] else "❌ FAIL"
        print(f"   {status}: {validation['check']}")
        print(f"      {validation['message']}")
    
    if all_passed:
        print(f"\n✅ All validations passed. Safe to execute {pipeline_name}")
    else:
        print(f"\n❌ Validation failures detected. Execution blocked for {pipeline_name}")
    
    return all_passed

# Example usage
# validate_before_execution('gold_trading_summary')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution Order

# COMMAND ----------

def get_execution_order():
    """
    Calculate optimal execution order based on dependencies (topological sort)
    """
    from collections import deque
    
    # Build adjacency list and in-degree count
    in_degree = {pipeline: 0 for pipeline in pipeline_dependencies}
    adj_list = {pipeline: [] for pipeline in pipeline_dependencies}
    
    for pipeline, deps in pipeline_dependencies.items():
        for upstream in deps['upstream']:
            adj_list[upstream].append(pipeline)
            in_degree[pipeline] += 1
    
    # Topological sort
    queue = deque([p for p in in_degree if in_degree[p] == 0])
    execution_order = []
    
    while queue:
        current = queue.popleft()
        execution_order.append(current)
        
        for neighbor in adj_list[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    return execution_order

execution_order = get_execution_order()

print("📋 Optimal Execution Order:")
for i, pipeline in enumerate(execution_order, 1):
    print(f"   {i}. {pipeline}")

# COMMAND ----------

print("✅ Dependency management ready")
