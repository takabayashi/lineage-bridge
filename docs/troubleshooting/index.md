# Troubleshooting Guide

Quick reference for common issues and solutions when using LineageBridge.

## Quick Reference

| Issue | Symptoms | Solution | Page |
|-------|----------|----------|------|
| **401 Unauthorized** | Extraction fails with "401" or "Unauthorized" | Check API key/secret format, verify permissions | [Credential Issues](credential-issues.md#401-unauthorized) |
| **403 Forbidden** | Extraction fails with "403" or "Forbidden" | Grant required permissions to API key | [Credential Issues](credential-issues.md#403-forbidden) |
| **400 Bad Request** | Extraction fails with "400" or "Bad Request" | Verify environment ID, check cluster access | [API Errors](api-errors.md#400-bad-request) |
| **429 Too Many Requests** | Extraction slow or fails with "429" | Wait for rate limit reset, reduce concurrency | [API Errors](api-errors.md#429-too-many-requests) |
| **500/502/503/504 Errors** | Intermittent extraction failures | Retry with exponential backoff (automatic) | [API Errors](api-errors.md#500-internal-server-error) |
| **Timeout** | Extractor hangs or times out after 120s | Check network, reduce scope, increase timeout | [Extraction Failures](extraction-failures.md#timeout-errors) |
| **Missing topics** | Topics don't appear in graph | Enable KafkaAdmin extractor, check cluster credentials | [Extraction Failures](extraction-failures.md#missing-topics) |
| **Missing connectors** | Connectors don't appear | Enable Connect extractor, verify environment ID | [Extraction Failures](extraction-failures.md#missing-connectors) |
| **Missing ksqlDB queries** | Queries don't appear | Enable ksqlDB extractor, verify cluster credentials | [Extraction Failures](extraction-failures.md#missing-ksqldb-queries) |
| **Missing Flink jobs** | Flink statements don't appear | Enable Flink extractor, verify organization ID | [Extraction Failures](extraction-failures.md#missing-flink-jobs) |
| **Missing schemas** | Schemas don't appear | Enable Schema Registry extractor, verify SR endpoint | [Extraction Failures](extraction-failures.md#missing-schemas) |
| **Missing catalog tables** | UC/Glue tables don't appear | Enable Tableflow extractor, check catalog credentials | [Extraction Failures](extraction-failures.md#missing-catalog-tables) |
| **Orphan nodes** | Nodes with no edges in graph | Expected in partial extractions, check enabled extractors | [Extraction Failures](extraction-failures.md#orphan-nodes) |
| **Slow extraction** | Extraction takes >5 minutes | Reduce scope, disable metrics, optimize network | [Performance](performance.md#slow-extraction) |
| **High memory usage** | UI crashes or freezes | Filter graph, reduce node count, export subgraph | [Performance](performance.md#high-memory-usage) |
| **Watcher not detecting changes** | Changes not triggering re-extraction | Check poll interval, verify watcher is running | [Performance](performance.md#watcher-issues) |

## Common Workflows

### First-Time Setup

1. Configure Cloud API key in `.env`:
   ```bash
   LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your_cloud_api_key
   LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your_cloud_api_secret
   ```

2. Test connectivity:
   ```bash
   uv run lineage-bridge-extract --env env-abc123
   ```

3. If you see 401/403 errors, see [Credential Issues](credential-issues.md).

### Debugging Missing Data

1. Check extractor status in UI sidebar — ensure all extractors are enabled.
2. Review extraction logs for warnings:
   ```bash
   uv run lineage-bridge-extract --env env-abc123 | grep "Warning"
   ```
3. Verify API permissions in Confluent Cloud UI.
4. See [Extraction Failures](extraction-failures.md) for extractor-specific debugging.

### Performance Optimization

1. Disable unused extractors (metrics, stream catalog).
2. Filter by specific cluster IDs:
   ```bash
   uv run lineage-bridge-extract --env env-abc123 --cluster lkc-xyz789
   ```
3. Export graph to JSON for offline analysis:
   ```bash
   uv run lineage-bridge-extract --env env-abc123 --output graph.json
   ```
4. See [Performance](performance.md) for optimization tips.

## Getting Help

- **GitHub Issues**: [https://github.com/takabayashi/lineage-bridge/issues](https://github.com/takabayashi/lineage-bridge/issues)
- **Documentation**: [https://lineage-bridge.readthedocs.io](https://lineage-bridge.readthedocs.io)
- **Logs**: Check console output and `.env` for debug settings

## Pages

- [Credential Issues](credential-issues.md) - API key problems, 401/403 errors
- [API Errors](api-errors.md) - API error codes and solutions
- [Extraction Failures](extraction-failures.md) - Timeout, missing data issues
- [Performance](performance.md) - Optimization tips
