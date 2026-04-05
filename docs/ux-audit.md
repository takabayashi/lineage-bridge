# LineageBridge UX Audit

**Auditor:** Prism (UI/UX specialist)
**Date:** 2026-04-04
**Scope:** Streamlit UI — `app.py`, `node_details.py`, `graph_renderer.py`, `styles.py`, `visjs_graph/`
**Context:** POC-stage product. Findings are prioritized for practical impact, not polish.

---

## 1. Information Architecture

### 1a. Sidebar is overloaded (P2)

The sidebar packs five expander sections (Connection, Infrastructure, Extractors, Key Provisioning, Graph Filters) plus Load Data. Once a user connects and discovers environments, the sidebar can easily exceed two full viewport heights of content. The "Environment API Keys" and "Cluster API Keys" nested expanders inside Infrastructure make it worse — selecting two environments with three clusters each creates 15+ input fields in the sidebar alone.

**Recommendation:** Move credential entry (per-cluster keys, per-env SR/Flink keys) to a modal dialog or a dedicated "Credentials" page. The sidebar should focus on the core flow: scope selection, extract, filter. Phase 3 workstream: **UI Redesign**.

### 1b. Step tracker only shows on empty state (P3)

The 3-step tracker (Connect, Select Infrastructure, Extract) in the empty-state hero is good onboarding, but it disappears once the graph loads. A persistent breadcrumb or compact status bar showing current connection + last extraction time would help orientation.

**Recommendation:** Add a compact status line at the top of the sidebar showing connection status and last extraction timestamp. Phase 3 workstream: **UI Polish**.

### 1c. Load Data section placement is confusing (P3)

"Load Data" sits at the bottom of the sidebar below Graph Filters. Users scanning top-to-bottom see Connection -> Infrastructure -> Extractors -> actions -> filters -> then suddenly file upload. The demo button also duplicates the hero-area demo button.

**Recommendation:** Move Load Data above the connection section, or into the empty state only. Remove the duplicate demo button. Phase 3 workstream: **UI Polish**.

---

## 2. Catalog Node UX (UC_TABLE, GLUE_TABLE)

### 2a. Catalog nodes are visually distinct — good (No action)

UC_TABLE uses a dark amber (#F57F17) database cylinder icon. GLUE_TABLE uses AWS dark blue (#0D47A1) with a multi-column table grid. These are distinct from each other and from TABLEFLOW_TABLE (dark green, single-column grid). The visual differentiation works well.

### 2b. UC_TABLE detail panel lacks enrichment info (P2)

The UC_TABLE detail panel (`node_details.py` lines 245-266) shows catalog, schema, table name, catalog_type, workspace_url, and database. However, it does not surface:
- Column count or column listing (if enriched by Databricks UC provider)
- Table format (Delta, Iceberg)
- Storage location
- Owner or created-by info
- Whether the table was enriched by the provider or just inferred from Tableflow

The GLUE_TABLE panel (lines 268-286) is even sparser — only database, table name, region, and a console link.

**Recommendation:** Add an "Enrichment Status" indicator (enriched vs. inferred-only) and surface additional catalog metadata when available. The `_known_keys` set at line 417 would need updating as new attributes flow through. Phase 3 workstream: **Catalog Integration**.

### 2c. Deep links work but only when attributes are present (P2)

`build_node_url` in `styles.py` dispatches to `DatabricksUCProvider().build_url()` and `GlueCatalogProvider().build_url()` correctly. However, `build_confluent_cloud_url` returns `None` for UC_TABLE and GLUE_TABLE (they fall through all the `if` checks). The `node_details.py` panel handles this by calling `build_node_url` as a fallback (line 67: `sel_node.url or build_node_url(sel_node)`), which is correct.

The issue: if `workspace_url` is missing from UC_TABLE attributes, `DatabricksUCProvider().build_url()` likely returns `None`, and the user sees no link at all — no explanation of why.

**Recommendation:** Show a "Deep link unavailable — workspace_url not configured" message when the URL cannot be built, rather than silently omitting the link. Phase 3 workstream: **Catalog Integration**.

### 2d. Tooltip for catalog nodes is minimal (P3)

The tooltip for UC_TABLE (graph_renderer.py lines 160-166) shows catalog name and either "Databricks UC" or catalog_type. The GLUE_TABLE tooltip is absent entirely from the type-specific section (no `elif ntype == NodeType.GLUE_TABLE` block).

**Recommendation:** Add GLUE_TABLE tooltip details (database, table, region). Enrich UC_TABLE tooltip with schema.table and format info. Phase 3 workstream: **Catalog Integration**.

---

## 3. Graph Interaction

### 3a. Legend is complete (No action)

The legend in `_render_sidebar_graph_filters` iterates over all `NodeType` values and renders each with its SVG icon. All 10 node types are covered. Good.

### 3b. Schema nodes are hidden from graph — intentional but undiscoverable (P3)

Schema nodes are filtered out of the graph view (graph_renderer.py line 563: `if node.node_type == NodeType.SCHEMA: continue`) and shown as embedded info on topic nodes instead. This is a good design decision for reducing clutter, but the Schema type still appears in the legend and filter checkboxes with a count, which is misleading since those nodes will never appear in the graph.

**Recommendation:** Either hide Schema from the legend/filter list, or add a "(shown on topics)" annotation. Phase 3 workstream: **UI Polish**.

### 3c. Node selection requires click, no double-click differentiation (P3)

Single click selects a node and opens the detail panel. There is no double-click behavior (e.g., focus/zoom to that node's neighborhood). The "Focus on this node" action is buried at the bottom of the detail panel.

**Recommendation:** Add double-click to focus behavior in the vis.js component. Phase 3 workstream: **Graph Interaction**.

### 3d. Region zoom is a strong feature (No action)

The Shift+drag region zoom in the vis.js component is well-implemented with visual feedback (dashed rectangle), keyboard shortcut, and toolbar toggle. The Fit All and +/- buttons work correctly.

### 3e. Edge labels can overlap on dense graphs (P2)

All edges have text labels ("produces", "consumes", "transforms", etc.) positioned at the top (`font.align: "top"`). On dense graphs with many edges between the same node groups, labels pile up and become unreadable.

**Recommendation:** Hide edge labels by default and show them only on hover, or add a "Show edge labels" toggle in Graph Filters. Phase 3 workstream: **Graph Interaction**.

### 3f. No edge legend (P3)

The legend shows node types but not edge types. Edge visual encoding (solid vs. dashed, color differences) is not explained anywhere.

**Recommendation:** Add an edge type section to the legend with color swatches and dash patterns. Phase 3 workstream: **UI Polish**.

---

## 4. Sidebar Usability

### 4a. Extract vs. Refresh button semantics are unclear (P2)

Two buttons: "Extract Lineage" (or "Re-extract") and "Refresh". "Refresh" re-runs with the last params, but the distinction is not obvious. Both do the same thing if params haven't changed.

**Recommendation:** Merge into a single "Extract" button. If params match last run, show a confirmation or just re-run. The "Refresh" button adds cognitive overhead without clear value. Phase 3 workstream: **UI Redesign**.

### 4b. Extractor toggles have smart defaults (No action)

Toggles are automatically disabled when the corresponding service is not discovered (e.g., Flink toggle disabled when no Flink pools exist). This is good behavior.

### 4c. Discovery flow requires manual clicks (P2)

The user must click "Discover" before they can select environments. This is a necessary step but feels like friction. The "Discover" button disables itself once all environments are discovered, which is a good touch.

**Recommendation:** Auto-discover on connect if the environment count is small (< 5). Phase 3 workstream: **UI Redesign**.

### 4d. Cluster multiselect defaults to all clusters (P3)

When environments are selected, all clusters within them are pre-selected (line 749: `default=list(all_cluster_options.keys())`). This is the right default for most users but could be surprising in multi-cluster environments.

**No action needed** — current behavior is correct for a POC.

---

## 5. Accessibility

### 5a. Color-only status encoding (P1)

Status indicators throughout the UI use color alone to convey state:
- Running/Active = green, Failed = red, Paused = amber (node_details.py)
- Metrics active/inactive = green/red dot (graph_renderer.py)
- Status badge icons use color circles with unicode symbols, but the symbols are very small

This fails WCAG 2.1 SC 1.4.1 (Use of Color). Users with color vision deficiency cannot distinguish running from failed states.

**Recommendation:** Add text labels alongside color indicators (e.g., show "RUNNING" as text, not just a green badge). The detail panel already does this partially (shows the state text), but the graph node badges rely on color + tiny unicode symbols. Phase 3 workstream: **Accessibility**.

### 5b. Small font sizes in tooltips and badges (P2)

Tooltip text is 10-11px. Badge text is 13px on a 22px diameter circle. Edge labels are 9px. These are below comfortable reading size, especially on high-DPI displays.

**Recommendation:** Increase minimum font size to 12px for body text and 14px for interactive elements. Phase 3 workstream: **Accessibility**.

### 5c. Keyboard navigation is partially supported (P3)

The vis.js component has `keyboard: { enabled: true }` in interaction options, which enables arrow-key panning and +/- zoom. However, there is no keyboard way to select nodes or navigate between them. Tab navigation within the Streamlit sidebar works via Streamlit defaults.

**Recommendation:** Add keyboard node navigation (Tab to cycle through nodes, Enter to select). Phase 3 workstream: **Accessibility**.

### 5d. No ARIA labels on custom HTML (P3)

The sidebar legend, detail panel, step tracker, and hero card all use `unsafe_allow_html=True` with raw HTML that lacks ARIA roles and labels. Screen readers will not interpret these elements correctly.

**Recommendation:** Add `role` and `aria-label` attributes to custom HTML blocks. Phase 3 workstream: **Accessibility**.

### 5e. Dark text on dark hero card (P3)

The hero card uses `color: #e0e0e0` on `background: #1a1a2e`. The contrast ratio is approximately 8.5:1, which passes WCAG AA. However, the subtext uses `color: #b0bec5` which drops to about 5.8:1 — still passing AA but borderline for extended reading.

**No action needed** for POC — this passes minimum standards.

---

## 6. Performance

### 6a. Full graph re-render on every interaction (P2)

Every Streamlit rerun (filter change, node click, panel close) triggers a full re-execution of `render_graph_raw`, which iterates all nodes and edges to rebuild the vis.js data. For a graph with 500+ nodes, this creates noticeable lag.

The vis.js component itself re-initializes on every render (`initGraph` destroys and recreates the network). Node positions are preserved via `sessionStorage`, which helps, but the full teardown/rebuild cycle is expensive.

**Recommendation:** Use `@st.cache_data` for `render_graph_raw` results keyed on filter state hash. For the vis.js component, consider a diff-based update instead of full re-initialization. Phase 3 workstream: **Performance**.

### 6b. Session state stores full graph object (P3)

`st.session_state.graph` holds the entire `LineageGraph` including all node/edge objects. For large graphs (1000+ nodes), this consumes significant memory and serialization time on every Streamlit rerun.

**Recommendation:** Consider storing only the serialized JSON and deserializing on demand, or using `st.cache_resource` for the graph object. Phase 3 workstream: **Performance**.

### 6c. DAG layout is computed on every render (P2)

`_compute_dag_layout` runs the full Sugiyama algorithm (layer assignment + 4 barycenter sweeps) on every page render. For 200+ nodes this is noticeable.

**Recommendation:** Cache the layout computation keyed on the set of visible node IDs + edges. Phase 3 workstream: **Performance**.

### 6d. No lazy loading or pagination for large graphs (P3)

If a graph has 2000 nodes, all 2000 are sent to the vis.js component at once. vis.js handles this reasonably well up to ~1000 nodes but degrades beyond that.

**Recommendation:** For POC scope, add a warning when node count exceeds 500. For production, implement virtual viewport rendering or level-of-detail clustering. Phase 3 workstream: **Performance**.

---

## 7. Missing Features

### 7a. No graph search highlighting (P2)

The search box filters the graph to matching nodes + their neighborhood, but matched nodes are not visually highlighted. The user sees a subset of the graph but cannot tell which nodes matched vs. which are just neighbors.

**Recommendation:** Add a highlight ring or glow effect to nodes that directly match the search query. Phase 3 workstream: **Graph Interaction**.

### 7b. No undo/redo for graph state (P3)

Changing filters, focusing on a node, or clearing focus is a one-way operation. There is no back button or undo stack.

**Recommendation:** Implement a simple state history (last 5 filter states) with back/forward navigation. Phase 3 workstream: **UI Redesign**.

### 7c. No graph comparison (P3)

There is no way to compare two extraction runs to see what changed (new nodes, removed edges, attribute changes).

**Recommendation:** Store previous graph in session state and offer a diff view highlighting additions/removals. Phase 3 workstream: **Graph Interaction**.

### 7d. No node grouping or clustering (P2)

In multi-environment or multi-cluster deployments, nodes from different clusters are interleaved in the layout. There is no visual grouping by cluster or environment.

**Recommendation:** Add optional cluster/environment grouping with bounding boxes or color-coded background regions in the vis.js component. Phase 3 workstream: **Graph Interaction**.

### 7e. No full-text search across node attributes (P3)

Search only matches on `display_name` and `qualified_name`. Users cannot search by connector class, SQL content, tag name, or other attributes.

**Recommendation:** Extend search to match against `attributes` values and `tags`. Phase 3 workstream: **UI Polish**.

### 7f. Missing GLUE_TABLE tooltip details (P2)

As noted in 2d, the `render_graph_raw` tooltip builder has no `elif ntype == NodeType.GLUE_TABLE` case. GLUE_TABLE nodes show the generic tooltip with only the node name and location — no database, table name, or region info.

**Recommendation:** Add a GLUE_TABLE tooltip block mirroring the UC_TABLE one. This is a straightforward code addition. Phase 3 workstream: **Catalog Integration**.

---

## Summary by Priority

| Priority | Count | Key Items |
|----------|-------|-----------|
| **P1** | 1 | Color-only status encoding (accessibility) |
| **P2** | 10 | Sidebar overload, catalog detail gaps, GLUE tooltip missing, edge label clutter, performance (re-render, layout cache), node grouping, search highlighting, Extract/Refresh confusion, auto-discover |
| **P3** | 11 | Step tracker persistence, Load Data placement, schema legend annotation, double-click focus, edge legend, keyboard nav, ARIA labels, undo/redo, graph comparison, full-text search, lazy loading |

## Summary by Phase 3 Workstream

| Workstream | Findings |
|------------|----------|
| **Catalog Integration** | 2b, 2c, 2d, 7f |
| **UI Redesign** | 1a, 4a, 4c, 7b |
| **UI Polish** | 1b, 1c, 3b, 3f, 7e |
| **Graph Interaction** | 3c, 3e, 7a, 7c, 7d |
| **Accessibility** | 5a, 5b, 5c, 5d |
| **Performance** | 6a, 6b, 6c, 6d |
