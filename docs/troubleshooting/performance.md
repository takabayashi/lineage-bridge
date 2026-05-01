# Performance Optimization

Tips for improving extraction speed, reducing memory usage, and troubleshooting watcher issues.

## Slow Extraction

### Symptoms

- Extraction takes >5 minutes
- Specific phases are slow (check logs for timing)
- UI becomes unresponsive during extraction

### Performance Characteristics

**Typical extraction time**:
- Small environment (1 cluster, 10 topics): ~10s
- Medium environment (3 clusters, 100 topics): ~30s
- Large environment (10 clusters, 1000 topics): ~2-3 min

If extraction is significantly slower, optimization may help.

### Solutions

#### Disable Unused Extractors

Metrics and Stream Catalog add overhead:

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_metrics=False,          # Optional, can be slow
    enable_stream_catalog=False,   # Optional
)
```

In UI, uncheck these in the sidebar.

#### Reduce Extraction Scope

Extract one cluster at a time:

```bash
uv run lineage-bridge-extract --env env-abc123 --cluster lkc-xyz789
```

#### Skip Enrichment

Extract without catalog enrichment:

```bash
uv run lineage-bridge-extract --env env-abc123 --no-enrich
```

Enrich later:

```bash
uv run lineage-bridge-extract --enrich-only --output lineage_graph.json
```

#### Reduce Metrics Lookback

Metrics lookback default is 1 hour. Reduce it:

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_metrics=True,
    metrics_lookback_hours=0.25,  # 15 minutes
)
```

#### Use Caching

Cache extraction results to avoid re-fetching:

```bash
# Export to JSON
uv run lineage-bridge-extract --env env-abc123 --output graph.json

# Load from JSON in UI
# (Not currently supported in UI, use CLI)
```

#### Optimize Network

- Run extraction from same region as Confluent Cloud environment
- Use wired connection instead of WiFi
- Check for VPN overhead

Test network latency:

```bash
curl -w "time_total: %{time_total}s\n" -o /dev/null -s https://api.confluent.cloud
```

## High Memory Usage

### Symptoms

- UI crashes or freezes
- Browser tab becomes unresponsive
- Python process uses >2GB RAM

### Memory Characteristics

**Approximate memory usage**:
- 100 nodes: ~50MB
- 1,000 nodes: ~200MB
- 10,000 nodes: ~1GB

Graph rendering in UI (vis.js) adds overhead.

### Solutions

#### Filter Graph

Use filters in UI sidebar:
- Filter by node type (e.g., only topics and connectors)
- Filter by environment/cluster
- Hide orphan nodes
- Search for specific nodes

#### Export Subgraph

Extract a subset of the graph:

```python
# Filter by cluster
graph_filtered = LineageGraph()
for node in graph.nodes:
    if node.cluster_id == "lkc-xyz789":
        graph_filtered.add_node(node)

for edge in graph.edges:
    if edge.src_id in graph_filtered._nodes and edge.dst_id in graph_filtered._nodes:
        graph_filtered.add_edge(edge)

graph_filtered.to_json_file("graph_filtered.json")
```

#### Reduce Node Count

- Extract fewer environments/clusters
- Disable heavy extractors (connectors, schemas)
- Use cluster filter

#### Increase Browser Memory

Chrome/Edge:

```bash
google-chrome --js-flags="--max-old-space-size=4096"
```

Firefox: `about:config` → `javascript.options.mem.max` → `4096`

#### Use CLI Instead of UI

For large graphs, use CLI and export to JSON:

```bash
uv run lineage-bridge-extract --env env-abc123 --output graph.json
```

Analyze JSON with scripts or tools like `jq`.

## Watcher Issues

### Watcher Not Detecting Changes

#### Symptoms

- Watcher running but changes not triggering re-extraction
- Changes take >1 minute to reflect

#### Causes

1. **Poll interval too long** - Default 10s
2. **Debounce cooldown** - 30s cooldown after last change
3. **Change not in polled resources** - Watcher only polls topics, connectors, ksqlDB, Flink

#### Solutions

**Verify watcher is running**:

```bash
# CLI
uv run lineage-bridge-watch

# UI
Check sidebar: "Watcher" toggle should be ON
```

**Check logs**:

```
INFO ChangePoller detected change: topics in cluster lkc-xyz789
INFO WatcherEngine triggered extraction due to changes
```

**Understand debounce**:

Watcher waits 30s after last change before extracting. This batches rapid changes.

```
10:00:00 - Change detected: topic created
10:00:05 - Change detected: connector created
10:00:35 - Extraction triggered (30s after last change)
```

**Manually trigger extraction**:

In UI, click "Extract" button in sidebar.

### Watcher Using Too Much CPU

#### Symptoms

- High CPU usage when watcher is running
- Python process uses 100% CPU

#### Causes

1. **Poll interval too short** - Default 10s is reasonable
2. **Large number of resources** - Polling 1000s of topics
3. **API throttling** - Hitting rate limits

#### Solutions

**Increase poll interval**:

Edit `watcher/engine.py`:

```python
_POLL_INTERVAL_SECONDS = 30  # Increase from 10
```

**Reduce scope**:

Watcher polls all clusters in all configured environments. To watch fewer resources:

```bash
# .env
LINEAGE_BRIDGE_WATCHER_ENVIRONMENTS=env-abc123  # Comma-separated
LINEAGE_BRIDGE_WATCHER_CLUSTERS=lkc-xyz789      # Comma-separated
```

(Not currently implemented, but can be added.)

**Disable watcher**:

If not needed, turn off watcher:

```bash
# UI: Toggle off in sidebar
# CLI: Don't run lineage-bridge-watch
```

### Watcher Not Stopping

#### Symptoms

- Watcher toggle stuck in ON state
- Python thread won't stop

#### Solutions

**Kill watcher thread**:

In UI, refresh the page. Watcher runs in background thread and will stop on page refresh.

In CLI, Ctrl+C stops the watcher.

**Check for hung processes**:

```bash
ps aux | grep lineage-bridge-watch
kill -9 <PID>
```

## Graph Rendering Performance

### Symptoms

- UI freezes when rendering graph
- Graph layout takes >10 seconds
- Pan/zoom is laggy

### Solutions

#### Use Hierarchical Layout

Hierarchical layout (Sugiyama) is faster than force-directed:

```python
# Default in UI
layout = "hierarchical"
```

#### Reduce Node Count

Filter graph before rendering (see High Memory Usage).

#### Disable Physics

For large graphs, disable physics simulation:

```javascript
// In visjs_graph component
physics: {
  enabled: false
}
```

(Requires custom component modification.)

#### Export to Image

For static analysis, export graph to PNG:

```python
# Not currently supported, but can be added with networkx
import matplotlib.pyplot as plt
import networkx as nx

nx.draw(graph._graph, with_labels=True)
plt.savefig("graph.png")
```

## API Rate Limiting

### Symptoms

- Extraction slow due to 429 errors
- Many retry warnings in logs

### Solutions

See [API Errors: 429 Too Many Requests](api-errors.md#429-too-many-requests).

## Database Performance (Catalog Enrichment)

### Symptoms

- Catalog enrichment slow (Phase 4b)
- Databricks/Glue API calls timing out

### Solutions

#### Skip Enrichment

```bash
uv run lineage-bridge-extract --env env-abc123 --no-enrich
```

#### Optimize Databricks

- Use serverless SQL warehouse (faster cold start)
- Ensure warehouse is RUNNING before extraction

```bash
# .env
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456  # Pre-warm warehouse
```

#### Optimize AWS Glue

- Use same region as LineageBridge
- Increase AWS CLI timeout:

```bash
# ~/.aws/config
[default]
cli_read_timeout = 60
```

#### Optimize Google BigQuery

- Use same region as LineageBridge
- Increase ADC timeout (not currently configurable)

## Profiling and Debugging

### Enable Performance Logging

```bash
# .env
LINEAGE_BRIDGE_LOG_LEVEL=DEBUG
```

This logs timing for each phase:

```
INFO Phase 1/4 complete: 12.3s
INFO Phase 2/4 complete: 5.6s
INFO Phase 3/4 complete: 3.2s
INFO Phase 4/4 complete: 8.1s
```

### Profile Python Code

Use `cProfile` to profile extraction:

```bash
python -m cProfile -o extraction.prof -m lineage_bridge.extractors.orchestrator

# Analyze profile
python -m pstats extraction.prof
```

### Measure API Latency

Use `httpx` logging:

```python
import logging
logging.getLogger("httpx").setLevel(logging.DEBUG)
```

This logs request/response times:

```
DEBUG HTTP Request: GET https://api.confluent.cloud/... "HTTP/1.1 200 OK"
```

## Resource Limits

### Python

- **Max graph size**: ~100K nodes (limited by memory)
- **Max file size**: ~100MB JSON (limited by disk I/O)
- **Max concurrent requests**: Limited by httpx (default 100 connections)

### Browser (UI)

- **Max nodes rendered**: ~5,000 (limited by vis.js performance)
- **Max memory**: ~2GB (limited by browser tab)

### Confluent Cloud API

- **Rate limit**: ~1000 requests/hour per API key
- **Page size**: 100 items per page (default)
- **Response timeout**: 30s per request

## Optimization Checklist

- [ ] Disable unused extractors (metrics, stream catalog)
- [ ] Filter by specific cluster IDs
- [ ] Skip enrichment (`--no-enrich`)
- [ ] Reduce metrics lookback window
- [ ] Use same region as Confluent Cloud
- [ ] Filter graph in UI before rendering
- [ ] Export subgraph for large datasets
- [ ] Check API rate limits
- [ ] Enable debug logging to identify bottlenecks

## Next Steps

- [Extraction Failures](extraction-failures.md) - Debugging incomplete results
- [API Errors](api-errors.md) - Handling rate limits and retries
- [Architecture: Extraction Pipeline](../architecture/extraction-pipeline.md) - Understanding phases
