# Graph Visualization Guide

The LineageBridge graph visualization is an interactive directed acyclic graph (DAG) powered by vis.js. This guide covers graph interaction, filtering, search, and layout customization.

## Overview

The graph displays lineage as a **directed graph** with:

- **Nodes** — Resources (topics, connectors, queries, tables)
- **Edges** — Relationships (produces, consumes, transforms, materializes)
- **Colors** — System-based (Confluent blue, Databricks orange, AWS orange, External gray)
- **Layout** — Sugiyama-style DAG layout with minimal edge crossings

Access the graph:

1. Launch the Streamlit UI: `lineage-bridge-ui`
2. Connect and extract lineage
3. The graph appears in the **Lineage Graph** tab

---

## Graph Interaction

### Mouse Controls

| Action | Behavior |
|--------|----------|
| **Drag** | Move nodes around the canvas |
| **Mouse wheel** | Zoom in/out |
| **Ctrl/Cmd + wheel** | Zoom faster |
| **Click node** | Select node, open detail panel |
| **Shift + drag** | Region select (multi-select) |
| **Double-click node** | Center on node |
| **Click canvas** | Deselect all nodes |

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| **Esc** | Clear selection |
| **Arrow keys** | Pan the canvas |
| **+/-** | Zoom in/out |

### Touch Controls (Mobile/Tablet)

| Gesture | Behavior |
|---------|----------|
| **Pinch** | Zoom in/out |
| **Two-finger drag** | Pan the canvas |
| **Tap node** | Select node |
| **Double-tap** | Center on node |

---

## Node Types

Nodes are color-coded by system and node type:

### Confluent Nodes

| Type | Icon | Color | Description |
|------|------|-------|-------------|
| **Kafka Topic** | 🔷 | Blue | Kafka topic |
| **Connector** | 🔌 | Green | Source or sink connector |
| **ksqlDB Query** | 🔄 | Purple | ksqlDB persistent query |
| **Flink Job** | ⚡ | Orange | Flink SQL statement |
| **Tableflow Table** | 📊 | Teal | Tableflow topic → table mapping |
| **Schema** | 📝 | Gray | Avro/Protobuf/JSON schema |
| **Consumer Group** | 👥 | Indigo | Kafka consumer group |
| **External Dataset** | 🌐 | Blue-gray | External system referenced by connector |

### Databricks Nodes

| Type | Icon | Color | Description |
|------|------|-------|-------------|
| **UC Table** | 🏛 | Orange | Unity Catalog table |

### AWS Nodes

| Type | Icon | Color | Description |
|------|------|-------|-------------|
| **Glue Table** | 🗄 | Orange | AWS Glue table |

### Node Labels

Node labels show the **display name**:

- Kafka topics → topic name
- Connectors → connector name
- UC tables → `catalog.schema.table`
- Glue tables → `database.table`

---

## Edge Types

Edges represent relationships between nodes:

| Type | Style | Color | Description |
|------|-------|-------|-------------|
| **PRODUCES** | Solid | Blue | Source produces data to destination |
| **CONSUMES** | Solid | Green | Source consumes data from destination |
| **TRANSFORMS** | Solid | Purple | Source transforms destination (ksqlDB/Flink) |
| **MATERIALIZES** | Solid | Orange | Source materializes to destination (Tableflow) |
| **HAS_SCHEMA** | Dashed | Gray | Topic has schema |
| **MEMBER_OF** | Dashed | Indigo | Consumer group member of topic |

**Edge direction:** Arrows point from source to destination.

Example flow:

```
postgres-source (Connector) → orders (Topic) → order-enrichment (Flink) → orders_enriched (Topic)
```

---

## Filtering

The sidebar provides multiple filtering options to focus on specific parts of the graph.

### Environment Filter

**Sidebar → Graph → Filters → Environment**

Filter nodes by Confluent Cloud environment:

```
Environment: All
```

Or select a specific environment:

```
Environment: Production (env-abc123)
```

Only nodes from the selected environment are shown.

### Cluster Filter

**Sidebar → Graph → Filters → Cluster**

Filter nodes by Kafka cluster:

```
Cluster: All
```

Or select a specific cluster:

```
Cluster: lkc-xyz789
```

Only nodes from the selected cluster are shown.

### Node Type Filters

**Sidebar → Graph → Filters**

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

**Use cases:**

- **Hide schemas** — Reduce clutter when you only care about data flow
- **Show only topics** — Focus on Kafka topology
- **Show only UC tables** — See lakehouse lineage

### Search by Name

**Sidebar → Graph → Filters → Search**

Enter a substring to filter nodes by qualified name:

```
Search by name: orders
```

The graph shows only nodes whose qualified name contains "orders":

- `orders`
- `orders_enriched`
- `pending_orders`

Search is **case-insensitive** and matches anywhere in the name.

### Hide Disconnected Nodes

**Sidebar → Graph → Filters**

```
☑ Hide disconnected nodes
```

When enabled, hides nodes with no edges (no upstream or downstream connections).

**Use case:** Large graphs with many isolated nodes (common after filtering).

---

## Focus Mode

Focus mode isolates a single node and its neighborhood.

### How to Use

1. Click a node in the graph
2. **Sidebar → Graph → Filters**
3. Enable **Focus on selected node**
4. Adjust **Hop radius** (1-5 hops)

The graph now shows only:

- The selected node
- Nodes within N hops (upstream and downstream)

### Example

**Selected node:** `orders` (Kafka Topic)

**Hop radius:** 2

**Visible nodes:**

- **1 hop upstream:** `postgres-source` (Connector)
- **1 hop downstream:** `order-enrichment` (Flink Job)
- **2 hops downstream:** `orders_enriched` (Topic)
- **2 hops upstream:** (none)

**Use cases:**

- **Trace upstream lineage** for a table ("where does this data come from?")
- **Trace downstream impact** for a topic ("who consumes this?")
- **Isolate a single pipeline** for debugging

### Disable Focus

Uncheck **Focus on selected node** to show the full graph again.

---

## Multi-Select

Select multiple nodes at once:

1. Hold **Shift**
2. Drag a rectangle around nodes
3. All nodes in the rectangle are selected
4. Detail panel shows the first selected node

**Use cases:**

- Group related nodes for visual analysis
- Identify clusters of related resources

---

## Layout

### DAG Layout

The graph uses a **Sugiyama-style hierarchical layout**:

- Nodes are arranged in layers (levels)
- Edges point downward (mostly)
- Edge crossings are minimized

The layout is computed server-side using `networkx`:

```python
from networkx import topological_generations

for level, node_group in enumerate(topological_generations(graph)):
    # Arrange nodes in horizontal rows
```

### Position Persistence

Node positions are saved in browser `localStorage` (keyed by graph version). Positions persist across:

- Browser refreshes
- UI restarts
- Graph reloads (same topology)

**When positions reset:**

- Graph topology changes (new nodes, removed nodes)
- You manually reset layout

### Manual Layout

Drag nodes to customize the layout. Positions are saved automatically.

### Reset Layout

**Sidebar → Graph → Filters**

```
[🔄 Reset Layout]
```

This clears saved positions and re-computes the DAG layout.

**Use when:**

- You've manually rearranged nodes and want to start over
- Graph topology changed and positions are misaligned

---

## Node Detail Panel

Click a node to open the detail panel on the right side.

### Panel Contents

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

### Sections

| Section | Description |
|---------|-------------|
| **Type** | Node type (Kafka Topic, Connector, UC Table, etc.) |
| **System** | System type (CONFLUENT, DATABRICKS, AWS, EXTERNAL) |
| **Environment** | Confluent Cloud environment ID and display name |
| **Cluster** | Kafka cluster ID |
| **Attributes** | Node-specific metadata (partitions, retention, config) |
| **Upstream** | Nodes that feed data to this node |
| **Downstream** | Nodes that consume data from this node |
| **Deep Link** | URL to view the resource in its native console |

### Deep Links

Deep links open the resource in its native console:

| Node Type | Link Destination |
|-----------|-----------------|
| Kafka Topic | Confluent Cloud topic page |
| Connector | Confluent Cloud connector page |
| ksqlDB Query | Confluent Cloud ksqlDB editor |
| Flink Job | Confluent Cloud Flink SQL workspace |
| UC Table | Databricks table explorer |
| Glue Table | AWS Glue console |
| Schema | Schema Registry subject page |

### Close Panel

Click **Close** or select another node to dismiss the panel.

---

## Graph Stats

The stats bar above the graph shows:

```
Nodes: 142    Edges: 238    Node Types: 7    Environments: 2    Clusters: 3    Pipelines: 12
```

| Metric | Description |
|--------|-------------|
| **Nodes** | Total node count (filtered) |
| **Edges** | Total edge count (filtered) |
| **Node Types** | Distinct node types in the graph |
| **Environments** | Distinct Confluent Cloud environments |
| **Clusters** | Distinct Kafka clusters |
| **Pipelines** | End-to-end flows (source connector → topic → sink connector) |

**Pipelines** are computed as paths from source connectors to sink connectors via topics.

---

## Export & Import

### Export JSON

**Header → Export JSON**

Downloads the current graph as JSON:

```json
{
  "nodes": [...],
  "edges": [...]
}
```

**Use cases:**

- Share graphs with teammates
- Archive lineage snapshots
- Load graphs in automation/CI/CD

### Import JSON

**Sidebar → Data → Load Data → Import JSON**

Upload a previously exported graph:

```
[Choose File] lineage_graph.json
```

The graph loads automatically.

---

## Tips & Tricks

### Performance with Large Graphs

- **500+ nodes?** Enable **Hide Disconnected Nodes**
- **Too many edges?** Filter by node type
- **Slow rendering?** Use environment/cluster filters

### Identify Bottlenecks

1. Filter to show only **Kafka Topics**
2. Look for topics with many downstream consumers
3. Click the topic to see consumer list in detail panel

### Trace Data Flow

1. Click a **source connector**
2. Enable **Focus on selected node** with hop radius 3+
3. See the full downstream pipeline

### Find Unused Resources

1. Enable **Hide disconnected nodes**
2. Toggle off all node types except one (e.g., Topics)
3. Disconnected nodes = topics with no producers/consumers

### Compare Environments

1. Extract lineage from two environments
2. Export JSON for each
3. Load in separate browser tabs
4. Compare topology visually

---

## Troubleshooting

### Graph is empty

**Check:**

1. Did extraction succeed? (Check sidebar → Extraction panel)
2. Are all node type filters enabled? (Sidebar → Graph → Filters)
3. Is the search box empty?
4. Are environment/cluster filters set to "All"?

### Nodes overlap

**Solutions:**

- Drag nodes manually to separate them
- Click **Reset Layout** to re-compute positions
- Enable **Hide disconnected nodes** to reduce clutter

### Edges are hard to follow

**Solutions:**

- Use **Focus mode** to isolate a single node
- Filter by node type to reduce edge count
- Click nodes to see upstream/downstream in detail panel

### Positions reset after reload

**Cause:** Graph topology changed (new nodes, removed nodes)

**Behavior:** Positions persist only for identical graphs. If the set of node IDs changes, layout resets.

### Can't find a specific node

**Solution:** Use **Search by name** (Sidebar → Graph → Filters → Search)

### Graph is slow

**Solutions:**

- Reduce node count via filters
- Disable physics (already disabled in LineageBridge)
- Use a smaller hop radius in Focus mode

---

## Next Steps

- **Learn UI features:** See [Streamlit UI Guide](streamlit-ui.md)
- **Automate extraction:** See [CLI Tools Reference](cli-tools.md)
- **Set up auto-update:** See [Change Detection Guide](change-detection.md)
