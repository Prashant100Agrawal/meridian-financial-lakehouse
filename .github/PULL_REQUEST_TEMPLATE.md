# Pull Request Template

## Summary
<!-- Brief description of changes -->

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update
- [ ] Pipeline/infrastructure change

## Affected Components
- [ ] Ingestion (02_ingestion)
- [ ] Operational layer (03_operational)
- [ ] Standardized layer (04_standardized)
- [ ] Reporting layer (05_reporting)
- [ ] Data quality (06_data_quality)
- [ ] Pipelines (07_pipelines)
- [ ] Orchestration (08_orchestration)
- [ ] Governance (12_governance)
- [ ] Monitoring (13_monitoring)
- [ ] Other

## Testing
- [ ] Unit tests pass
- [ ] DABs bundle validates (`databricks bundle validate`)
- [ ] Tested in dev environment
- [ ] No hardcoded secrets or credentials

## Checklist
- [ ] Code follows project conventions
- [ ] No secrets or credentials committed
- [ ] Schema names use business-centric naming (operational/standardized/reporting/regulatory)
- [ ] Logging via PipelineLogger where applicable
- [ ] Updated relevant documentation
