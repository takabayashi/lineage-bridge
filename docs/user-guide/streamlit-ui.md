# Streamlit UI Guide

The LineageBridge Streamlit UI is the primary interactive interface for discovering, extracting, visualizing, and exploring stream lineage from Confluent Cloud.

## Overview

The UI is a single-page Streamlit application with:

- **Sidebar** — Connection, infrastructure selection, extractors, filters, and legend
- **Main area** — Graph visualization and change watcher tabs
- **Node details** — Slide-in panel with attributes, neighbors, and deep links

Launch the UI:

```bash
lineage-bridge-ui
# or
uv run streamlit run lineage_bridge/ui/app.py
```

Access at: **http://localhost:8501**

---

## Getting Started

### 1. Connect to Confluent Cloud

**Sidebar → Setup → Connection**

On first launch, the Connection expander is open. You need:

- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY` — Cloud-level API key
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET` — Cloud-level API secret

If credentials are found in `.env`, the UI shows:

```
Credentials: abcd...xyz
[Connect]
```

Click **Connect** to authenticate. The UI will:

1. Validate credentials
2. List environments in your organization
3. Cache selections for future sessions

Once connected:

```
Connected — 3 environment(s)
```

The Connection expander collapses automatically.

### 2. Select Infrastructure

**Sidebar → Setup → Infrastructure**

After connecting, expand **Infrastructure** to select:

- **Environment(s)** — One or more Confluent Cloud environments
- **Cluster(s)** — Filter to specific Kafka clusters (optional)

The UI auto-discovers:

- All environments your API key can access
- All Kafka clusters in the selected environment(s)
- Schema Registry endpoints (if Stream Governance is enabled)

Example:

```
Environments:  ✓ Production (env-abc123)
               ✓ Staging (env-def456)
               ☐ Dev (env-ghi789)

Clusters:      All (default)
```

Selections are cached locally for the next session.

### 3. Extract Lineage

**Sidebar → Extraction**

Click **Extract Lineage** to run the extraction pipeline. The UI shows progress:

```
Phase 1/4: Extracting Kafka topics & consumer groups
Phase 2/4: Extracting connectors, ksqlDB, Flink
Phase 3/4: Enriching with schemas & catalog metadata
Phase 4/4: Extracting Tableflow & catalog integrations
Done: 142 nodes, 238 edges (CONFLUENT: 120, DATABRICKS: 15, AWS: 7)
```

**Extraction time:** Typically 10-30 seconds per environment, depending on:

- Number of clusters
- Number of topics, connectors, and queries
- Catalog enrichment enabled

### 4. Explore the Graph

After extraction, the main area shows the **Lineage Graph** tab with:

- **Stats bar** — Node count, edge count, environments, clusters, pipelines
- **Interactive graph** — Drag, zoom, click to inspect
- **Detail panel** — Attributes, neighbors, and deep links (on node click)

See [Graph Visualization Guide](graph-visualization.md) for interaction details.

---

## Sidebar Reference

The sidebar is organized into sections:

### Setup

#### Connection

**Status when connected:**

```
Connected — 3 environment(s)
Credentials: HCG5... (prefix)
[Disconnect]
```

Click **Disconnect** to clear credentials and reset the connection. This is useful when:
- Switching between different Confluent Cloud accounts
- Troubleshooting authentication issues
- Testing with different credential sets

**Status when disconnected:**

```
No credentials found.
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...
```

Expand to view masked credentials or reconnect. The UI shows the first 8 characters of your API key for verification.

#### Infrastructure

**Environment Selector**

Multi-select for environments:

```
☑ Production (env-abc123)
☑ Staging (env-def456)
☐ Dev (env-ghi789)
```

**Cluster Selector**

Optional filter:

```
Clusters: All
```

Or specific clusters:

```
Clusters: ✓ lkc-xyz789
          ☐ lkc-pqr456
```

**Discover Button**

Click **Discover Environments** to refresh the list from Confluent Cloud.

### Extraction

#### Extractors

**Sidebar → Extraction → Extractors**

Toggle individual extractors on/off:

| Extractor | Extracts | Default |
|-----------|----------|---------|
| **Kafka Admin** | Topics, consumer groups | Always on (required) |
| **Connect** | Connectors, external datasets | ✓ On |
| **ksqlDB** | Persistent queries | ✓ On |
| **Flink** | Flink SQL statements | ✓ On |
| **Schema Registry** | Schemas | ✓ On |
| **Stream Catalog** | Tags, business metadata | ✓ On |
| **Tableflow** | Topic → table mappings | ✓ On |
| **Catalog Enrichment** | UC/Glue table metadata | ✓ On |
| **Metrics** | Throughput, consumer lag | ☐ Off (slower) |

**When to disable extractors:**

- **Connect/ksqlDB/Flink off** — You only care about raw Kafka topology
- **Schema Registry off** — No schemas in use
- **Stream Catalog off** — Not using tags/business metadata
- **Tableflow off** — No lakehouse integration
- **Catalog Enrichment off** — Faster extraction, less metadata
- **Metrics on** — Live throughput data (adds ~5-10s per cluster)

#### Actions

**Extract Lineage** — Run the full pipeline

**Enrich Existing Graph** — Backfill catalog metadata and metrics on the current graph (no re-extraction)

**Load Demo Graph** — Generate a sample graph for UI exploration (no Confluent credentials needed)

### Publish

After extraction, the **Publish** section appears with catalog-specific controls.

#### Databricks

**Sidebar → Publish → Databricks**

Push lineage metadata to Unity Catalog tables:

- **Set Table Properties** — Write lineage as TBLPROPERTIES
- **Set Table Comments** — Write lineage as COMMENT
- **Create Bridge Table** — Write full lineage to a dedicated table

Requires:

- `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL`
- `LINEAGE_BRIDGE_DATABRICKS_TOKEN`
- `LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID` (or auto-discovery)

Click **Push Lineage to Databricks** to write metadata.

#### AWS Glue

**Sidebar → Publish → AWS Glue**

Push lineage metadata to Glue tables:

- **Set Table Parameters** — Write lineage as table parameters
- **Set Table Description** — Write lineage as description

Requires:

- `LINEAGE_BRIDGE_AWS_REGION`
- AWS credentials (boto3 default credential chain)

Click **Push Lineage to Glue** to write metadata.

#### Google Data Lineage

**Sidebar → Publish → Google Data Lineage**

Push lineage as OpenLineage events to Google Data Lineage API:

Requires:

- `LINEAGE_BRIDGE_GCP_PROJECT_ID`
- `LINEAGE_BRIDGE_GCP_LOCATION`
- Google Application Default Credentials

Click **Push Lineage to Google** to send events.

### Graph

After extraction, the **Graph** section appears with filters and legend.

#### Filters

**Sidebar → Graph → Filters**

**Environment Filter**

```
Environment: All
```

Or filter to one environment:

```
Environment: Production (env-abc123)
```

**Cluster Filter**

```
Cluster: All
```

Or filter to one cluster:

```
Cluster: lkc-xyz789
```

**Search**

```
Search by name: orders
```

Enter any substring to filter nodes by qualified name.

**Focus Node + Hop Limit**

```
☑ Focus on selected node
Hop radius: 2
```

When a node is selected and focus is enabled, the graph shows only:

- The selected node
- Nodes within N hops (upstream and downstream)

**Hide Disconnected Nodes**

```
☑ Hide disconnected nodes
```

When enabled, hides nodes with no edges (useful for large graphs).

**Node Type Filters**

Toggle visibility by node type:

```
☑ Kafka Topics
☑ Connectors
☑ ksqlDB Queries
☑ Flink Jobs
☑ Tableflow Tables
☑ UC Tables
☑ Glue Tables
☑ Schemas
☑ External Datasets
☑ Consumer Groups
```

Uncheck to hide nodes of that type.

#### Legend

**Sidebar → Graph → Legend**

**Node Types**

Color-coded list of node types with icons:

```
🔷 Kafka Topic (blue)
🔌 Connector (green)
🔄 ksqlDB Query (purple)
⚡ Flink Job (orange)
📊 Tableflow Table (teal)
🏛 UC Table (databricks orange)
🗄 Glue Table (aws orange)
📝 Schema (gray)
🌐 External Dataset (blue-gray)
👥 Consumer Group (indigo)
```

**Edge Types**

Edge direction and style:

```
→ PRODUCES (solid blue)
→ CONSUMES (solid green)
→ TRANSFORMS (solid purple)
→ MATERIALIZES (solid orange)
⤏ HAS_SCHEMA (dashed gray)
⤏ MEMBER_OF (dashed indigo)
```

### Data

#### Load Data

**Sidebar → Data → Load Data**

**Import JSON**

Upload a previously exported lineage graph:

```
[Choose File] lineage_graph.json
```

After upload, the graph loads automatically.

**Export JSON**

Download the current graph:

```
[📥 Export JSON]
```

Downloads as `lineage_graph.json`.

**Load Demo Graph**

Generate a sample graph for testing:

```
[⚡ Load Demo Graph]
```

No Confluent credentials needed — useful for:

- UI demos
- Screenshot generation
- Feature testing

---

## Main Area

The main area has two tabs:

### Lineage Graph

The **Lineage Graph** tab shows:

#### Header

```
🌐 Lineage Graph    Last extracted: 2026-04-30 14:23:45
                    [📥 Export JSON]
```

#### Stats Bar

```
Nodes: 142    Edges: 238    Node Types: 7    Environments: 2    Clusters: 3    Pipelines: 12
```

- **Pipelines** — Count of end-to-end flows (source connector → topic → sink connector)

#### Graph Canvas

Interactive vis.js graph with:

- **Drag** — Move nodes
- **Zoom** — Mouse wheel or pinch
- **Click** — Select a node (opens detail panel)
- **Shift+drag** — Region select (multi-select)
- **Double-click** — Center on node

Positions are persisted in browser `localStorage`.

#### Node Detail Panel

When a node is selected, the right side shows:

```
──────────────────────────────────
📋 orders
──────────────────────────────────

Type:        Kafka Topic
System:      CONFLUENT
Environment: Production (env-abc123)
Cluster:     lkc-xyz789

Attributes:
  partitions: 6
  replication_factor: 3
  retention_ms: 604800000

Upstream (2):
  → postgres-source (Connector)
  → customer-stream (ksqlDB Query)

Downstream (3):
  → orders-sink (Connector)
  → order-enrichment (Flink Job)
  → analytics-cg (Consumer Group)

🔗 View in Confluent Cloud

[✕ Close]
```

**Deep Links:**

- Kafka topics → Confluent Cloud topic page
- Connectors → Confluent Cloud connector page
- UC tables → Databricks table explorer
- Glue tables → AWS Glue console

Click **Close** or select another node to dismiss.

### Change Watcher

The **Change Watcher** tab provides controls for the background watcher.

See [Change Detection Guide](change-detection.md) for details.

---

## Features

### Auto-Save Selections

The UI caches these selections in `~/.lineage_bridge/ui_cache.json`:

- Selected environment IDs
- Selected cluster IDs
- Extractor toggles
- Filter states

On next launch, selections are restored.

### Graph Layout Persistence

Node positions are saved in browser `localStorage` (keyed by graph version). The layout persists across:

- Browser refreshes
- UI restarts
- Graph reloads (same topology)

To reset positions:

**Sidebar → Graph → Filters**

```
[🔄 Reset Layout]
```

This clears saved positions and re-computes the DAG layout.

### Extraction Progress

During extraction, a status indicator shows:

```
⏳ Extracting...

Phase 1/4: Extracting Kafka topics & consumer groups
✓ KafkaAdmin:lkc-xyz789

Phase 2/4: Extracting connectors, ksqlDB, Flink
✓ Connect:lkc-xyz789
⚠ Warning: Extractor 'ksqlDB' got 401 Unauthorized
✓ Flink

Phase 3/4: Enriching with schemas & catalog metadata
✓ SchemaRegistry
✓ StreamCatalog

Phase 4/4: Extracting Tableflow & catalog integrations
✓ Tableflow

Done: 142 nodes, 238 edges (CONFLUENT: 120, DATABRICKS: 15, AWS: 7)
```

Warnings are logged but do not fail extraction.

### Focus Mode

**Sidebar → Graph → Filters → Focus on selected node**

When enabled:

1. Click a node in the graph
2. The graph filters to show only:
   - The selected node
   - Nodes within N hops (upstream and downstream)
3. Adjust hop radius to expand/contract the neighborhood

**Use cases:**

- Trace upstream lineage for a specific table
- See downstream consumers of a topic
- Isolate a single pipeline

### Demo Mode

Click **Load Demo Graph** to generate a sample graph:

- 30+ nodes across all node types
- Realistic topology (connectors → topics → transformations → tables)
- No Confluent credentials needed

Perfect for:

- UI walkthroughs
- Screenshot generation
- Feature demos

---

## Tips & Tricks

### Performance

- **Large graphs (500+ nodes)?** Enable **Hide Disconnected Nodes** and use filters
- **Slow extraction?** Disable **Metrics** extractor (saves 5-10s per cluster)
- **Too many edges?** Filter by node type to reduce visual clutter

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| **Shift+drag** | Region select |
| **Esc** | Clear selection |
| **Ctrl/Cmd + mouse wheel** | Zoom in/out |

### Multi-Select Workflow

1. Hold **Shift**
2. Drag a rectangle around nodes
3. All nodes in the rectangle are selected
4. Detail panel shows the first selected node

### Export for CI/CD

Extract in the UI, then export JSON for automation:

1. Extract lineage
2. Click **Export JSON** in header
3. Use the JSON in scripts, CI/CD, or API calls

### Refresh After Changes

If you make changes in Confluent Cloud (new topics, connectors, etc.):

1. Go to **Sidebar → Extraction**
2. Click **Extract Lineage** to refresh
3. Or enable the **Change Watcher** for automatic updates

---

## Troubleshooting

### "No credentials found"

**Solution:** Add credentials to `.env`:

```bash
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-secret
```

Restart the UI.

### "401 Unauthorized"

**Cause:** Cloud-level API key cannot access cluster-scoped resources (topics, ksqlDB, etc.)

**Solution:** Add cluster-scoped API key:

```bash
LINEAGE_BRIDGE_KAFKA_API_KEY=cluster-key
LINEAGE_BRIDGE_KAFKA_API_SECRET=cluster-secret
```

Or use auto-provisioning (see [Configuration](../getting-started/configuration.md)).

### "No Schema Registry endpoint found"

**Cause:** Stream Governance is not enabled for the environment

**Solution:**

- Enable Stream Governance in Confluent Cloud
- Or set `LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT` manually

### Graph is empty after extraction

**Check:**

1. Do you have topics in the selected clusters?
2. Are all extractors enabled?
3. Check console logs for errors (sidebar → Extraction → progress panel)

### Positions reset after every load

**Cause:** Graph topology changed (new nodes, removed nodes)

**Behavior:** Layout resets when the set of node IDs changes. Positions persist only for identical graphs.

### UI is slow with large graphs

**Solutions:**

- Enable **Hide Disconnected Nodes**
- Filter by environment/cluster
- Use **Focus on selected node** with low hop radius
- Disable unused node types

---

## Next Steps

- **Learn graph interaction:** See [Graph Visualization Guide](graph-visualization.md)
- **Automate extraction:** See [CLI Tools Reference](cli-tools.md)
- **Set up auto-update:** See [Change Detection Guide](change-detection.md)
- **Push lineage to catalogs:** See [Catalog Integration Guides](../catalog-integration/index.md)
