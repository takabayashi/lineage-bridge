# UX Redesign Proposal

**Author:** Prism (UI/UX specialist)
**Date:** 2026-05-02
**Branch context:** `experiment/ui-redesign` (no code changes — exploratory)
**Scope:** Heuristic audit + framework comparison (Streamlit improved vs React/FastAPI) + phased Streamlit-only refactor plan
**Reads from:** `docs/ux-audit.md` (2026-04-04 baseline), `docs/decisions.md` (ADR-020/021/022/023 redirect ownership of state and contracts)

The 2026-04-04 audit captured the surface-level pain (sidebar sprawl, missing accessibility, color-only status). This proposal does not re-litigate those — it builds on top of them. Where a finding is already in the prior audit and unchanged, this doc cites it (`UA-Nx`) and moves on. Most of what follows is new ground: the post-Phase-2E sidebar regression (`sidebar/__init__.py` is now an 8-section accordion), the `extraction_log` / `_log_source` pattern, the watcher tab's broken contract, and the architectural reality that the API layer (`/api`) makes a React frontend a near-greenfield exercise rather than a port.

---

## Section 1: Audit Findings

### 1.1 Top bar / chrome (app shell)

**What works**
- `app.py:35-40` keeps the page config minimal and `layout="wide"` is correct for a graph-first tool.
- `app.py:80` introduces a top-level `st.tabs(["Lineage Graph", "Change Watcher"])` split, which is the right primary navigation choice now that watcher is a peer feature.
- The header row at `app.py:101-119` correctly puts the "Last extracted" timestamp next to the title and the JSON export action on the right.

**Pain points**
- **P1 — There is no app shell, just a sidebar plus tabs.** `app.py:73-86` has no global status strip, no per-environment context indicator, and no breadcrumbs. Once a graph loads, the user has lost sight of *which* environment / cluster the graph corresponds to (it's only inferable from the stats bar at `app.py:122-130`, which counts but does not name). The prior audit flagged this as P3 (`UA-1b`); on a redesign branch it's worth promoting because the tab switch makes it more acute — when you come back from the Change Watcher tab, there is no header anchor that tells you which extraction's graph you're looking at. Heuristic: Nielsen #1 (visibility of system status).
- **P2 — Tab labels are flat strings with no count badges.** `app.py:80`. The "Change Watcher" tab gives no indication that it is running, has new events, or is idle. Users must click into it to know. Heuristic: Nielsen #1.
- **P2 — The footer at `app.py:226-239` is rendered for every page state, including the empty state.** It mixes copyright, contact email, build credits, and a "give it a star" CTA into one centered block. On the empty state hero, this is visual competition with the primary CTA ("Load Demo Graph"). Recommendation: hide the footer on empty state; reduce to one line everywhere else.

**Why it matters**
The product is fundamentally a "what am I looking at" tool. Without a stable header that names the connection + environment + extraction time, every interaction starts from "wait, where am I?". This is the single biggest information-architecture cost in the current shell.

---

### 1.2 Sidebar

**What works**
- The Phase 2E decomposition (`sidebar/__init__.py`, `connection.py`, `scope.py`, `credentials.py`, `actions.py`, `filters.py`) is a clean code split — each file does one thing. Composition in `sidebar/__init__.py:52-143` is readable.
- The four-stage section grouping ("Setup", "Extraction", "Publish", "Graph", "Data") at `sidebar/__init__.py:60, 83, 95, 113, 140` is correct conceptually — it mirrors the user's mental model of "configure → run → publish → explore → load".
- `sidebar/scope.py:30-61` correctly makes Discover the primary action when nothing is discovered yet, and demotes it to secondary once everything is.
- `sidebar/filters.py:79-100` two-column checkbox grid for node-type filters is dense and scanable.
- `sidebar/actions.py:127-156` three-up button row (Extract / Enrich / Refresh) is appropriately compact, and disabling Enrich/Refresh on missing prerequisites is good affordance work.

**Pain points**
- **P0 — The sidebar is now an 8-expander accordion, worse than the 5 the prior audit flagged.** `sidebar/__init__.py` opens these expanders in order: Connection, Infrastructure, Extractors, Databricks, AWS, Google Data Lineage, Filters, Legend, Load Data — plus the always-visible Connect/Disconnect, Discover, Extract/Enrich/Refresh, Push log, status badges. The prior audit (`UA-1a`) called the 5-expander version "P2"; the post-Phase-2E version regressed and the catalog publish split made it worse. With a connected user mid-extraction, the sidebar can be 4+ viewport heights tall. Heuristic: Nielsen #8 (aesthetic and minimalist design); recognition over recall (the user has to scroll to remember what's available).
- **P0 — Credential entry is buried two expanders deep and replicated per-env / per-cluster.** `sidebar/credentials.py:30-93` puts SR endpoint + key + secret + Flink key + secret per environment, all inside a nested `st.expander("Environment API Keys")` inside `Infrastructure`. With 2 envs, that's 10 password inputs in the sidebar. With 2 envs and 3 clusters each, add 6 more (`credentials.py:96-129`). The prior audit (`UA-1a`) recommended moving these to a modal — that recommendation was *not* implemented; instead the inputs remain inline. This is the single worst usability issue in the sidebar. Heuristic: Nielsen #8.
- **P1 — Push to UC / Glue / DataZone / Google duplicates the same pattern four times.** `sidebar/actions.py:245-480` has four near-identical render functions, each with their own discover-warehouse / select-warehouse / push-options / push-button flow. The user sees three expander labels in the sidebar ("Databricks", "AWS", "Google Data Lineage") with no indication of which ones are configured or which apply to the current graph. The visibility logic only fires *inside* the expander (e.g., `actions.py:248-250` "Set `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL` in .env to enable"). Recommendation: a single "Publish" panel that lists targets with a status pill (configured / not configured / no eligible nodes) and an inline push button.
- **P1 — `_resolve_extraction_context` (`sidebar/actions.py:36-105`) re-reads ~12 session-state keys on every sidebar render.** Every Streamlit rerun (any widget change anywhere) re-runs this function. It's cheap individually but is one of many "this runs on every keystroke" patterns that contribute to perceived sluggishness on large graphs. Heuristic: Nielsen #7 (efficiency of use) — system feel.
- **P1 — Connection status badge is rendered twice.** `sidebar/__init__.py:64-71` renders the green "Connected — N environment(s)" badge at the sidebar root, and `sidebar/connection.py:20-22` renders a `st.info("Credentials: ...")` line *and* a "Connection active." caption inside the Connection expander. Two badges saying the same thing. Heuristic: Nielsen #8.
- **P2 — "Discover" vs "Refresh" buttons in `scope.py:32-61` are visually identical and adjacent.** Refresh re-runs discovery for *all* environments unconditionally. There is no confirmation, no diff feedback, no spinner that explains what happened beyond a progress bar that disappears. A user who clicks Refresh by mistake has just wasted N async API calls. Heuristic: Nielsen #5 (error prevention).
- **P2 — Extract/Enrich/Refresh trio replays the prior audit's `UA-4a` problem at higher cost.** `sidebar/actions.py:127-156`. Now there are *three* buttons with overlapping semantics. Refresh = re-extract with last params. Re-extract (when graph exists) = extract with current UI state. Enrich = enrich the existing graph. The differences between Re-extract and Refresh are non-obvious from the labels and tooltips alone. Worse, both extraction and refresh will rebuild the graph from scratch and reset focus/selection (`actions.py:184-186, 224-226`), but Enrich preserves them — that's a real semantic difference the user cannot infer.
- **P2 — Filters expander in graph view is `expanded=True` by default (`sidebar/__init__.py:132`) but Legend is `expanded=False` (`:134`).** Inconsistent. Both are equally referenceable. Default expanded for both, or default collapsed and let the user open them.
- **P2 — "Load Data" lives at the bottom of the sidebar, below the Graph section.** Same finding as `UA-1c`. After 6+ commits of UI work, this hasn't been addressed. The duplicate "Load Demo Graph" button (one in `sidebar/actions.py:550-555` *and* one in the empty state `empty_state.py:182-193`) is also still present.

**Why it matters**
The sidebar is the control panel. When it grows to 4 viewport heights and hides the most-used controls (extract, push) below 5+ closed expanders that don't communicate their state, it becomes a configuration menu instead of a control panel. Every additional click between the user's intent and the action degrades flow.

---

### 1.3 Graph canvas (vis.js component + DAG layout)

**What works**
- The vis.js component (`components/visjs_graph/index.html`) is solid: keyboard navigation, region zoom (`index.html:247-339`), Fit All / zoom in/out / hint text, dark mode auto-detection (`index.html:152-184`), session-storage position persistence (`index.html:128-150, 222-227`).
- Sugiyama layout (`graph_renderer.py:382-495`) gives deterministic, crossing-minimised positions. The 4-sweep barycenter is a real engineering investment that pays off.
- Per-catalog brand icons (`styles.py:348-371`) are now distinct between UC, Glue, BigQuery — `UA-2a` rated this "good" before; the refinement to per-catalog brand SVGs is even better.
- DLQ + schema badge variants on Kafka topics (`graph_renderer.py:580-588`) communicate role-without-text, which is appropriate for icons.

**Pain points**
- **P1 — Full re-render on every Streamlit interaction.** `app.py:150-159, 176-200`. Every click into a sidebar widget rebuilds the vis.js dataset and re-initializes the network (`index.html:210-213`). Position is preserved via sessionStorage but layout, tooltips, and node objects are all reconstructed. For a 500-node graph this is the single biggest perceived-latency cost. Same finding as `UA-6a`; not addressed.
- **P1 — `_compute_dag_layout` (`graph_renderer.py:382-495`) runs on every render.** Same as `UA-6c`. The 4-sweep barycenter pass on 200+ nodes is noticeable. There is no caching keyed on the visible-node-set hash.
- **P1 — Node click is the only way to interact.** `index.html:230-234` only listens for `click` and only emits `params.nodes[0]`. There is no double-click (focus), no right-click (context menu), no shift-click (multi-select). The "Focus on this node" action is buried at the very bottom of the detail panel (`node_details.py:572-578`). Heuristic: Nielsen #7 (efficiency of use).
- **P2 — The graph iframe is sized to a fixed `height=650` (`app.py:198`).** On a tall monitor it leaves dead space below; on a short laptop it forces scrolling. No responsive sizing, no "fill remaining viewport" option.
- **P2 — Edge labels still pile up on dense graphs.** `UA-3e` not addressed. `styles.py:641-648` still hardcodes `"size": 9, "align": "top"` for all edges with no hover-only mode.
- **P2 — Search highlighting is missing.** `UA-7a` not addressed. `graph_renderer.py:524-540` filters to matched nodes + their N-hop neighbourhood, but the matched nodes are not visually distinguished. Users see a subgraph and can't tell which node they were looking for.
- **P2 — The toolbar (`index.html:42-59`) is iframe-local.** Region Zoom, Fit All, +/- live in the iframe; the empty-canvas message ("No nodes match the current filters") at `app.py:162` lives in Streamlit. Two layers of UI, no integration. The hint text ("Click a node for details") sits inside the iframe and can't be customized when the user has selected something.
- **P3 — The fixed `border: 1px solid rgba(128,128,128,0.2)` on `#graph-container` (`index.html:28`) clashes with the dark-mode override.** No dark-mode style for the border colour itself.
- **P3 — Mobile / narrow viewport: untested and probably broken.** `st.columns([3, 2])` (`app.py:167`) on narrow viewports collapses to single-column with the graph 3/5 wide and the detail panel 2/5 below — the detail panel becomes unreadable. No `st.session_state.viewport_*` detection because Streamlit doesn't expose it.

**Why it matters**
The graph is the product. Re-render cost compounds with sidebar interactions because every filter change rebuilds the vis.js network. This isn't visible at 50 nodes; it's painful at 500 and unusable at 2000.

---

### 1.4 Node details panel

**What works**
- Type-specific detail blocks (`node_details.py:142-368`) cover every NodeType including the post-ADR-021 collapsed `CATALOG_TABLE` with per-`catalog_type` branches at `:244-322`.
- Header gradient with type-coloured left border (`node_details.py:40-54`) gives strong visual identity without relying on color alone (label text at `:46-48` reinforces it).
- Schema definition expander (`:439-452`) with JSON pretty-printing is a real upgrade from the prior audit period.
- Upstream/downstream neighbour navigation (`:528-566`) with click-to-jump is the single best interaction in the current UI — turns the detail panel into a graph traversal tool.
- "Other Attributes" catch-all expander (`:518-521`) keeps the panel scanable while not hiding data.

**Pain points**
- **P1 — The panel is 587 lines and rebuilds top-to-bottom on every interaction within it.** `node_details.py:26-587`. Clicking an upstream/downstream button (`:544, 558`) sets `selected_node` then calls `st.rerun()` — the entire panel re-renders, the graph re-renders, the sidebar re-renders. For exploration ("walk 5 nodes upstream"), the user pays 5 full reruns.
- **P1 — Dismissal flow has 3 buttons doing 2 things.** `node_details.py:56-63` "Close", `:580-587` "Clear selection", `app.py:204-205` (clicking a dismissed node is a no-op). Close and Clear selection do exactly the same thing (set `_dismissed_node`, clear `selected_node`, rerun). Why two buttons? Why two locations?
- **P1 — `_known_keys` (`node_details.py:467-517`) is a manually-maintained denylist for the "Other Attributes" expander.** Every time a new attribute is added by a client, it shows up *both* in its dedicated section (if explicitly handled) *and* in Other Attributes (if not added to the set). Drift is silent. Recommendation: invert the model — Other Attributes shows everything *not* rendered by the type-specific block, and the type-specific block returns the set of keys it consumed.
- **P2 — Inline raw HTML for status colours and badges (`node_details.py:153-156, 187-190, 207-210, 225-228, 237-240, 357-360`) duplicates the `STATUS_BADGE_MAP` in `styles.py:459-479`.** Two sources of truth for "RUNNING is green". When the badge map changed in a prior commit, the detail panel didn't follow.
- **P2 — "Workspace" deep link uses raw `<a href>` HTML (`node_details.py:266-271`) but only when `workspace_url` is in attributes.** When missing, no fallback message, no "Deep link unavailable" hint. Same as `UA-2c`, partially addressed by `build_node_url` dispatch but the missing-link UX is still silent.
- **P2 — Tags bar (`:454-464`) and "Other Attributes" (`:518-521`) are both at the bottom, after Neighbours, after Schemas, after Metrics. By the time a user has scrolled past the upstream/downstream buttons they have left the cognitive thread.** Reorder: Resource Info → Location → Type-specific → Tags → Metrics → Schemas → Neighbours → Other Attributes → Actions. Tags belong with identity, not catch-all.
- **P3 — All neighbour buttons render with `width="stretch"` and identical visual weight (`:543-548, 557-562`).** A topic neighbour and a Flink job neighbour look identical except for the label. No type icon, no colour cue. The detail panel knows the node type — it should show a 16px icon next to each neighbour.

**Why it matters**
The detail panel is where users spend most of their attention once the graph loads. It is the de-facto search-result, lineage-walker, and metadata-viewer all in one. Optimising its rerun cost and its visual scanability has higher impact than any other surface.

---

### 1.5 Extraction / log panel

**What works**
- `st.status("Extracting lineage...", expanded=True)` (`sidebar/actions.py:178`) gives an inline collapsible progress widget. Good Streamlit-native pattern.
- The classifier `_classify_log_entry` (`sidebar/actions.py:493-519`) maps log labels to css classes (`log-warning`, `log-skip`, `log-phase`, `log-discovery`, `log-provision`) and adds an icon — this is real visual work that pays off.
- `extraction_log` is a session-state list of strings (`actions.py:176, 199, 217`) — simple, works across reruns, easy to inspect.

**Pain points**
- **P1 — Two different log streams share one session-state list (`extraction_log`) gated by `_log_source`.** `sidebar/actions.py:176-177, 200-201, 218-219, 323-324, 377-378, 416-417, 469-470` and rendering at `:236-239, 487-490`. The "Extraction Log" expander only shows lines if `_log_source == "extraction"`; "Push Log" only if `== "publish"`. So when a user runs an extraction, then pushes to UC, the extraction log *disappears* (the list is overwritten). There is no scroll-back, no history, no "show both". This is a P1 information-loss bug.
- **P1 — Inside `st.status(..., expanded=True)`, the log is rendered separately *under* the status widget via `_render_extraction_log()` inside an expander.** So the user sees: status widget (live progress lines from `st.status`) → expander labelled "Extraction Log" (the same lines, persisted) → no clear handoff between live and persisted. Heuristic: Nielsen #1 (visibility of system status).
- **P1 — The progress callback `_ui_progress()` (`extraction.py:60-67`) appends to the log on every phase update, but Streamlit doesn't refresh the log expander mid-extraction.** The user sees the `st.status` lines stream, but the persisted log only shows up *after* the action completes and `st.rerun()` fires. So if extraction takes 30 seconds, the log expander is stale-empty for those 30 seconds.
- **P2 — Log entries are styled but not searchable / filterable.** `_classify_log_entry` (`actions.py:493-519`) classifies into 5 css classes, but the rendered log (`actions.py:522-542`) has no filter chip ("show only warnings", "skip phases"). On a 200-line extraction log, finding the 3 warnings means scrolling.
- **P2 — Log lines mix markdown (`**Label**`) with prose.** `actions.py:497-505` parses the bolded prefix back out. This is a workaround for not having structured payloads. ADR-020 explicitly calls out typed `ProgressCallback` payloads as the future fix. Until then, this string-parsing layer is the source of `_classify_log_entry`'s fragility (the classifier looks for `"warning" in label_lower` substring matches — false positives possible).
- **P3 — No log download / copy.** A user who hits an error and wants to file a bug has to screenshot or hand-copy lines.

**Why it matters**
Extraction is slow (multi-second to multi-minute) and users *will* tab away. When they come back, the log is the only record of what happened. Today, that record gets clobbered by the next push action, and it's only visible after the action finishes. This actively works against debugging.

---

### 1.6 Empty state

**What works**
- The hero card (`empty_state.py:21-38`), feature grid (`:42-83`), and step tracker (`:156-173`) collectively explain the product in roughly 3 viewport heights. Good for a first-time user.
- Architecture flow (`:88-136`) communicates the data path visually (`Confluent → Clients → Orchestrator → Graph → UI`), which sets the right mental model.
- Step tracker dynamically updates state (`:147-154`) — once the user connects, step 1 turns green; once they select an env, step 2 turns green. Real onboarding feedback.

**Pain points**
- **P2 — The hero card is *hardcoded dark* (`styles.css:58-70`) regardless of theme.** The card has `background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0d2137 100%)` and `color: #e0e0e0`. In light mode this looks intentional and on-brand; in dark mode it looks fine; but `color: #b0bec5` on the paragraph drops contrast (matches `UA-5e`'s observation about borderline AA passing).
- **P2 — Five "Get started" surfaces compete.** `empty_state.py:144-193` — heading "Get started" → step tracker → instructional sentence ("Use the **sidebar** to connect...") → centered "Load Demo Graph" button → and the user *also* sees the sidebar with its own Connect button. Same call-to-action three different ways. Heuristic: Nielsen #8 (minimalist).
- **P2 — Demo button is duplicated three places.** Hero `empty_state.py:182-193`, sidebar `actions.py:550-555`, *and* implicitly via "Load from path / Upload JSON" in `actions.py:565-598`. `UA-1c` flagged the duplication; the sidebar duplicate is still there.
- **P3 — All four feature cards have the same visual weight.** No primary feature distinction. A first-time user can't tell whether "Auto-Discovery" or "Catalog Integration" is the headline value.
- **P3 — Flat-text "Phase 1/2/3/4" callouts under the architecture flow (`empty_state.py:110-132`) duplicate `CLAUDE.md`'s extraction phase description.** This is internal-documentation language leaking into the user-facing UI. Users don't care which phase Tableflow is in; they care what it does.

**Why it matters**
First-impression and onboarding. The empty state is currently *over-built* (4 explanatory sections + 3 CTAs), which dilutes the primary action. The sidebar is the control panel; the empty state should drive the user there in one step.

---

### 1.7 Watcher controls

**What works**
- The `st.tabs(["Lineage Graph", "Change Watcher"])` split (`app.py:80`) puts the watcher at peer-level with the graph, which is the right IA after watcher graduated from a sidebar toggle.
- Auto-refreshing fragment (`watcher.py:229-294` `@st.fragment(run_every=5)`) gives live event feed without full app rerun. This is the most modern Streamlit pattern in the codebase.
- Status badge with pulse animation (`watcher.py:297-335`) gives an at-a-glance sense of "watcher is alive". Good visual affordance.

**Pain points**
- **P0 — The "Audit Log" detection mode is wired into the UI (`watcher.py:51-97, 165-189`) but ADR-014 superseded the Kafka audit consumer with REST polling.** The audit-log path requires bootstrap server + key + secret credentials that almost no user has (the audit log cluster is private, 2-key limit). Users will pick this option, fill in fields, get a cryptic error. The mode toggle should default to REST Polling and make Audit Log an "advanced" hidden option, or remove it until the audit consumer comes back.
- **P0 — `watcher.py:14-17` and `:374-383` import and instantiate `WatcherEngine` directly from the UI thread, and `:215-223` reads `engine.last_graph` / `engine._use_audit_log` (private attribute access at `:149`) directly off the engine object.** ADR-023 has explicitly called this pattern out as the dual-mode hazard and is rewriting it to a service + repository + API model. So everything in `watcher.py` is on borrowed time, but as long as it ships in the UI, the bug it produces is real: two Streamlit instances spawn two watcher threads polling Confluent independently. Not a UX bug per se but a system bug visible from the UI.
- **P1 — The Push-UC / Push-Glue checkboxes in the watcher controls (`watcher.py:124-127`) silently change behaviour after extraction.** When a watcher cycle fires, it does an extract and *then* pushes. The user sets-and-forgets; later they realise their UC tables have new lineage entries. There is no log surface in the watcher tab showing the push happened. Heuristic: Nielsen #1.
- **P1 — Layout is unbalanced.** `watcher.py:48` uses `st.columns([2, 3, 2])` — the middle column is empty (`_, _, c_right`). Visual gutter, but most users will read the layout as "controls on left, status on right", missing that the middle column is dead space.
- **P2 — "Mode: Audit Log (Kafka)" caption only appears *after* the engine starts (`watcher.py:147-152`).** Pre-start, the mode is implied by the radio toggle at the top — but during operation, the only mode indicator is buried in the right column. It should be in the status badge.
- **P2 — Event Feed is a `st.dataframe` with 6 columns (`watcher.py:249-254`).** The `ID` column is a UUID-ish string nobody reads. The `Environment` and `Cluster` columns are mostly empty for poll-mode events. Rows wrap awkwardly. A list-of-cards layout (Time + change icon + resource name + tiny env/cluster pill) would scan better.
- **P2 — Extraction History table (`watcher.py:289-294`) and Event Feed (`:249-254`) both use raw `st.dataframe` with `height=min(400, 35 + 35 * len(rows))` — for 1 row, 70px; for 10 rows, 385px. The fragment refreshes every 5s, and the height jumps as new events arrive — visual jitter.

**Why it matters**
The watcher is the headline new feature. Its current UI is functional but treats users as if they understand the underlying Kafka/REST distinction (they don't), and it loses information (push results) silently. The pulse animation and fragment-based refresh are good; the rest needs work.

---

### 1.8 Cross-cutting concerns

**P1 — Dark mode consistency**
- The vis.js component does runtime dark-mode detection (`index.html:152-184`) — solid.
- The hero card (`styles.css:58-70`) is fixed-dark — works in both themes by accident.
- Detail panel header gradient (`node_details.py:40-54`) uses `{ncolor}22` (12% opacity) on `{ncolor}08` (3%) — invisible in dark mode for cool colours (the Confluent blue palette).
- Status badges in `node_details.py` (`:154, 187, 207, 225, 237, 357, 388`) use raw colour strings like `#4CAF50`, `#F44336`, `#FF9800` — no `prefers-color-scheme` adjustment.

**P1 — Color-only status encoding**
- `UA-5a` flagged this; partially addressed via badge letter overlays in `styles.py:457-479` (▶/✓/✗/■/?). But the detail panel still relies on color alone for state (`node_details.py:186-189, 207-209, 224-227, 357-359`). The badge map and detail panel are out of sync.

**P1 — No keyboard navigation between widgets**
- vis.js has `keyboard: { enabled: true }` (`index.html:205`) but only for arrow-key panning. There is no Tab-cycle through nodes, no Enter-to-select. Streamlit's native widgets get default keyboard support but custom HTML blocks (legend, badges) don't have ARIA roles.

**P2 — Mobile / narrow viewport untested**
- The 3:2 column split (`app.py:167`) collapses awkwardly. The sidebar is overlay on mobile but the graph iframe at fixed 650px height eats the viewport. No responsive breakpoints.

**P2 — Loading states inconsistent**
- Connect uses `st.status("Connecting...", expanded=True)` (`connection.py:56`).
- Discover uses `st.progress(0)` (`scope.py:41`).
- Extract uses `st.status("Extracting lineage...", expanded=True)` (`actions.py:178`).
- Discover Warehouses uses `st.spinner("Discovering...")` (`actions.py:257`).
- Four different patterns for the same conceptual thing ("waiting for an async API"). Pick one (status-with-log) and apply across.

**P2 — Error states inconsistent**
- Connection failure: `status.update(label=f"Failed: {exc}", state="error")` (`connection.py:73`).
- Discovery failure: silently caches the exception string into `cache[env.id]` (`scope.py:46-47`).
- File load failure: `st.error(f"File not found: {p}")` (`actions.py:573`).
- Extraction failure: same pattern as connection (`actions.py:194-195`).
- No central error pattern; some failures are loud, some silent.

---

## Section 2: Prioritized Improvement List

All items are implementable in Streamlit unless explicitly noted. Effort: S = ≤1 day, M = 2–4 days, L = 1+ week.

### Sidebar
1. **P0 / M — Move credentials to a modal dialog.** Add a `st.dialog` for SR endpoint + per-env SR/Flink keys, and a separate dialog for per-cluster keys. Sidebar gets one "Manage credentials" button per env that shows configured-or-not status. Removes 10–16 password inputs from the sidebar. (Files: `sidebar/credentials.py`, `sidebar/scope.py`)
2. **P0 / S — Collapse all sidebar expanders into 4 max.** Setup, Run, Publish, Explore. Move Discover into the Setup expander; move all extractor toggles into Run; merge Filters + Legend into Explore. (Files: `sidebar/__init__.py`)
3. **P1 / S — Replace 3-button Extract/Enrich/Refresh with 1 primary + dropdown.** Primary `Run extraction` button; chevron-dropdown for Re-extract / Enrich / Refresh-with-last-params. Cuts cognitive load and reclaims sidebar real estate. (Files: `sidebar/actions.py`)
4. **P1 / M — Single Publish panel with status pills per target.** One expander labelled "Publish" containing 4 rows (UC / Glue / DataZone / Google) each with: configured / not-configured pill, eligible-node count, push button. Eliminates 3 nested expanders and the duplicate per-target settings. (Files: `sidebar/actions.py:245-480`)
5. **P1 / S — Remove duplicate "Load Demo" button.** Empty state keeps the demo button; sidebar Load Data drops it (only file upload + path remains). (Files: `sidebar/actions.py:550-555`)
6. **P2 / S — Single connection badge (sidebar root only).** Drop the in-expander "Connection active." caption and `st.info` repeat. (Files: `sidebar/connection.py:20-22`)
7. **P2 / S — Filters expander default to collapsed; remember last open state in session.** (Files: `sidebar/__init__.py:132`)

### Graph canvas
8. **P1 / M — Cache `_compute_dag_layout` result keyed on visible-node-id-set hash.** Use `@st.cache_data` with a frozenset of (node_ids, edges) as key. Cuts the 200ms+ layout cost on every interaction. (Files: `graph_renderer.py:382`)
9. **P1 / M — Cache `render_graph_raw` output keyed on `(graph_version, filter_hash, search, focus, hops, env_filter, cluster_filter, hide_disconnected)`.** Same `@st.cache_data` pattern. Removes the per-rerun rebuild. (Files: `graph_renderer.py:498`)
10. **P1 / S — Add double-click to focus in vis.js component.** `network.on("doubleClick", ...)` → emit a special `{action: "focus", id}` payload back to Streamlit. Backwards-compatible with the existing single-click. (Files: `components/visjs_graph/index.html:230-234`, `app.py:202-209`)
11. **P1 / S — Highlight matched nodes when search is active.** Add a `match: true` flag to nodes that directly match the search; in the iframe, give them a wider border or a glow filter. (Files: `graph_renderer.py:524-540`, `index.html`)
12. **P2 / M — Hide edge labels by default, show on hover only.** Add a "Show edge labels" toggle in Filters; default off; when off, vis.js renders edges without labels but tooltips still work. Resolves `UA-3e`. (Files: `styles.py:641-648`, `index.html`)
13. **P2 / M — Responsive graph height.** Set the iframe to fill `100vh - <known-chrome-height>` instead of fixed 650; expose a "Compact / Tall" toggle. (Files: `app.py:198`, `index.html:121`)
14. **P3 / S — Dark-mode aware container border in the iframe.** Read `isDark` (already computed at `:152-170`) and pick `rgba(255,255,255,0.1)` instead of `rgba(128,128,128,0.2)`. (Files: `index.html:28`)

### Node details
15. **P1 / M — Render the panel as fragments, not full-page reruns on neighbour click.** `@st.fragment` around the type-specific block + neighbour buttons. Walking 5 nodes upstream goes from 5 full reruns to 5 fragment reruns. (Files: `node_details.py`)
16. **P1 / S — Single dismiss action.** Remove "Close" button at top; keep "Clear selection" at bottom only. Or vice versa. Not both. (Files: `node_details.py:56-63, 580-587`)
17. **P1 / M — Invert `_known_keys` to a "consumed keys" return.** Each type-specific block returns the set of keys it rendered; "Other Attributes" shows the complement. Eliminates drift. (Files: `node_details.py:467-517`)
18. **P2 / S — Centralise status badge rendering.** Pull color + label + symbol from `STATUS_BADGE_MAP` (`styles.py:459`) instead of hardcoding `phase_color = "#4CAF50" if phase == "RUNNING" else "#FF9800"` six times. (Files: `node_details.py:186-189, 207-209, 224-227, 357-359`)
19. **P2 / S — Show "Deep link unavailable — set workspace_url" instead of silent absence.** When `build_node_url(sel_node)` returns None for a CATALOG_TABLE, render a dim caption explaining why. (Files: `node_details.py:314-322`)
20. **P2 / S — Reorder sections.** Identity → Location → Type-specific → Tags → Metrics → Schemas → Neighbours → Other Attributes → Actions. Tags belong next to identity. (Files: `node_details.py:454-464` move up)
21. **P3 / S — Add 16px node-type icon to neighbour buttons.** `node_details.py:543-548, 557-562`. The detail panel knows `nb.node_type`; render `icon_for_node(nb)` in front of the label. (Files: `node_details.py`)

### Extraction / log
22. **P1 / M — Split `extraction_log` into `extraction_log` + `push_log` (separate session-state lists).** Both render at the same time when both have content. Drop the `_log_source` gate. Eliminates the information-loss bug. (Files: `sidebar/actions.py:176-177, 199, 217, 323-324, 377-378, 416-417, 469-470`, `extraction.py:62-67`)
23. **P1 / S — Live-refresh the persisted log expander during extraction using `@st.fragment(run_every=1)`.** While the action is in flight, the log expander streams new lines instead of staying empty. (Files: `sidebar/actions.py:236-239`)
24. **P2 / S — Add log filter chips (All / Phases / Warnings only / Errors only) at the top of the log expander.** Uses `_classify_log_entry`'s css class to filter. (Files: `sidebar/actions.py:522-542`)
25. **P2 / S — Add "Copy log" + "Download as .txt" buttons inside the log expander.** Trivial Streamlit `st.download_button`. Helps bug reports. (Files: `sidebar/actions.py:522-542`)
26. **P3 / Blocked-by-ADR-020 — Adopt the typed `ProgressCallback` payloads.** Once ADR-020's services layer ships its `PhaseStarted` / `PhaseCompleted` / `Warning` / `Error` payloads, retire `_classify_log_entry`'s string parsing. (Files: `extraction.py:60-67`, `sidebar/actions.py:493-519`)

### Empty state
27. **P1 / S — Cut the empty state from 4 sections to 2.** Hero (with one inline "Connect" / "Load Demo" CTA pair) → Step tracker. Remove the feature grid and architecture flow (move to a `/docs` link). The user comes here to start, not to read marketing. (Files: `empty_state.py`)
28. **P2 / S — Replace "Phase 1/2/3/4" callouts with a single sentence.** "LineageBridge runs five extraction phases in parallel and renders them as a graph." Done. (Files: `empty_state.py:108-132`)
29. **P2 / S — Hide the global footer on empty state.** Footer competes with the primary CTA. (Files: `app.py:226-239`)

### Watcher
30. **P0 / S — Default detection mode to REST Polling; gate Audit Log behind a "Show advanced" toggle.** Until ADR-014's audit-consumer path comes back, Audit Log is a trap. (Files: `watcher.py:51-65`)
31. **P0 / Blocked-by-ADR-023 — Stop reading `engine.last_graph` and `engine._use_audit_log` from the UI.** ADR-023 is rewriting this to API + repository. Until then, mark the file with a `# DEPRECATED — see ADR-023` comment so the next dev knows. (Files: `watcher.py`)
32. **P1 / S — Show push results in the watcher event feed.** When a watch cycle pushes to UC/Glue, append a "Pushed N tables" event to `event_feed`. Currently silent. (Files: `watcher.py:204-227`, `watcher/engine.py`)
33. **P2 / S — Replace `st.dataframe` event feed with a card list.** Time + change-icon + resource name + dim env/cluster pill. Drop the `ID` column. (Files: `watcher.py:236-254`)
34. **P2 / S — Drop the empty middle column.** `st.columns([2, 3, 2])` → `st.columns([2, 2])`. (Files: `watcher.py:48`)

### Top bar / chrome
35. **P1 / S — Add a global status strip below the header.** Single line: "● Connected to <env-name> (<env-id>) · Cluster: <cluster-name> · Last extracted 12:34 UTC · 247 nodes". Survives tab switches. (Files: `app.py:73-86`)
36. **P2 / S — Add a count badge to the "Change Watcher" tab when events are pending.** `st.tabs(["Lineage Graph", f"Change Watcher ({n} new)"])`. (Files: `app.py:80`)
37. **P2 / S — Reduce footer to one line; hide on empty state.** (Files: `app.py:226-239`)

### Cross-cutting
38. **P1 / M — Single loading-state pattern.** Pick `st.status` everywhere. Remove `st.spinner` (`actions.py:257`) and bare `st.progress` (`scope.py:41`). (Files: `sidebar/scope.py:41-49, 53-61`, `sidebar/actions.py:257`)
39. **P1 / M — Single error-display pattern.** All failures route through one `_render_error(exc, context)` helper that shows error + a "Copy details" button. (Files: any `except Exception as exc` site)
40. **P2 / M — ARIA labels on all custom HTML blocks.** Legend (`filters.py:178-189`), edge legend (`:194-206`), status badges (`sidebar/__init__.py:64-71`), step tracker (`empty_state.py:156-173`). Each gets `role` + `aria-label`. (Files: as listed)

---

## Section 3: Framework Evaluation — Streamlit (improved) vs React + FastAPI

### Streamlit (improved)

**What's possible**
- Everything in Section 2 lands in Streamlit without leaving its idioms. `@st.fragment`, `@st.cache_data`, `st.dialog`, custom HTML components — these are mature features.
- The custom vis.js component (`components/visjs_graph/`) already proves bidirectional Streamlit ↔ JS works well enough for a graph canvas.
- Theme-awareness, responsive sidebar, and the status-strip pattern are all standard Streamlit territory.

**What's blocked**
- **Real-time updates without full reruns.** `@st.fragment(run_every=N)` is the closest approximation but cannot do server-pushed updates. The watcher's 5-second poll is the upper bound. WebSockets would require a custom component per panel.
- **Granular state isolation.** When the user types in the search box, *every* function in the script reruns. `@st.fragment` and `@st.cache_data` mitigate, but you can't truly isolate a panel from its parent's reruns.
- **Multi-panel layouts beyond simple columns.** Floating panels, resizable splitters, dockable side panels — not idiomatic. Possible with custom HTML + components but you're fighting Streamlit's render model.
- **Routing / deep-linkable state.** Streamlit has `st.query_params` (recent addition) but it's manual. Linking to "graph view focused on node X with filter Y" is awkward.
- **Drag-and-drop, complex interactions, modal-on-modal.** All possible-but-painful.
- **Performance ceiling on large graphs.** 1000+ nodes with full-page reruns is the wall, even with caching, because the JSON payload to the iframe is large.

**Ceiling of UX quality**
The Streamlit ceiling is roughly "polished internal tool". With Section 2's improvements applied, LineageBridge would be in the top quartile of Streamlit apps but would still feel different from a native web app: tabs reload, iframes have visible borders, live updates are polled not pushed, and complex interactions (drag-to-reorganise, multi-select, contextual menus) are absent. For an internal POC / demo / single-user diagnostic tool, this is *enough*. For a product that data engineers live in 4 hours a day, it isn't.

### React + FastAPI

**Architectural reality**
The FastAPI surface already exists (`/lineage_bridge/api/`, 25 endpoints across 7 routers — `meta`, `lineage`, `datasets`, `jobs`, `graphs`, `tasks`, `push`). ADR-015 designed it to speak OpenLineage outward AND accept events inward. ADR-020 is putting a service layer between the API and the orchestrator, so by the time a React frontend lands, the backend is already a clean API: extraction → `POST /tasks/extract` (returns task_id), poll → `GET /tasks/{id}`, fetch graph → `GET /graphs/{id}`, push → `POST /push` (designed in ADR-021's protocol v2). **This is not a port; it's a near-greenfield frontend on a stable backend.**

**What unlocks**
- **Real-time UI:** WebSockets for watcher event feed; live extraction progress without polling; granular toast notifications.
- **Component reuse and design system:** Use a mature React component library (Radix, shadcn/ui, MUI). Consistent loading / error / dialog patterns for free.
- **Routing / deep-linkable state:** `react-router` or Next.js routing. URL captures `graph_id + selected_node + filter_state` cleanly. Shareable links to "Slack me a link to this lineage view".
- **Performance:** React's diffing means filter changes don't re-serialise the whole graph payload. vis.js (or react-flow / Cytoscape.js) integrates as a React component with explicit lifecycle.
- **Accessibility:** Mature React component libraries ship with ARIA / keyboard / focus management baked in.
- **Multi-user & shareability:** Multiple users hit the same API; URLs are shareable; auth (already API-key-stub'd in `api/auth.py`) extends naturally.
- **Mobile / responsive:** Native CSS-in-JS / Tailwind; standard responsive patterns.
- **Testability:** Component tests (Vitest + React Testing Library) are fundamentally cheaper than Streamlit testing (which needs `streamlit.testing.v1.AppTest` and is brittle).

**Real costs**
- **Auth.** Today the API has a stub `auth.py` (1KB) — API key only. A real React app needs session management, login flow, token refresh, RBAC if multi-user. This is real work — call it 1–2 weeks per environment if you start from scratch.
- **State management.** TanStack Query for server state, Zustand / Jotai for client state. Standard patterns but they need to be set up.
- **Build tooling.** Vite + TypeScript + Tailwind + ESLint + Prettier + a CI pipeline that builds the frontend bundle. The repo has none of this today.
- **Deployment.** Today: `streamlit run`. After: serve the React bundle (S3 + CloudFront, or behind FastAPI as static files), CORS config on the API, separate Dockerfile. Adds a deployment unit.
- **Team skills.** The current crew (Scout/Blueprint/Weaver/Lens/Sentinel/Forge/Prism/Anvil) is Python-heavy. React + TypeScript is a new skill axis. Either hire / train, or accept slower frontend velocity.
- **Migration period.** Streamlit and React UIs cannot share state across reloads; a switchover means either a big-bang migration or a parallel-app phase where features land on both UIs.
- **No Python ↔ JS auto-binding.** Streamlit's "write Python, get UI" magic is gone. Every endpoint shape change means a TypeScript type update. OpenAPI schema generation (ADR-015's `docs/openapi.yaml` is already exported) would auto-generate the TS client — that's a partial win, but contract drift is a real ongoing cost.

**Migration path**
1. Stabilise the API (ADR-020 in flight). Lock the request/response shapes.
2. Generate a TypeScript client from `docs/openapi.yaml` (e.g., `openapi-typescript-codegen`).
3. Stand up a Vite + React + Tailwind shell with one read-only view (e.g., `GET /graphs/{id}` rendered with `react-flow` or vis.js-react).
4. Add the extraction trigger + polling flow.
5. Add the publish flow.
6. Add the watcher (WebSocket subscription to the watcher API per ADR-023).
7. Decommission Streamlit.

This is realistically 3–6 months of one engineer's time, longer if the engineer is also doing backend work.

### Decision matrix

Score 1 (worst) – 5 (best) per dimension. Equal weights for now; reweight in Section 4 of any future doc if priorities shift.

| Dimension | Streamlit (improved) | React + FastAPI |
|---|---|---|
| UX flexibility (custom panels, interactions, modals) | 2 | 5 |
| Dev velocity (time from idea to shipped feature) | 5 | 3 |
| Team skills (current crew is Python-heavy) | 5 | 2 |
| Deployment complexity (single binary vs frontend + API) | 5 | 3 |
| Real-time / streaming UI (watcher, live extraction progress) | 2 | 5 |
| Accessibility (WCAG AA across all surfaces) | 2 | 4 |
| Theming & design system (consistent visual language) | 3 | 5 |
| Custom components (graph canvas, log viewer, etc.) | 3 | 5 |
| **Total (out of 40)** | **27** | **32** |

### Recommendation

**Stay on Streamlit for now. Switch to React + FastAPI when the product crosses into "users live in this 4 hours a day" territory or when multi-user shareability becomes a hard requirement.**

The main tradeoff: Streamlit gives you 80% of the UX with 20% of the team-skills investment, *as long as the product is a single-user diagnostic / POC / demo tool*. React gives you a 30% UX improvement but at a 5–10× initial-investment cost and a permanent maintenance overhead in TypeScript and frontend tooling.

**Trigger condition for revisiting:**
- Any of these flips the calculus:
  - Multi-user / multi-tenant requirements (sharing graph URLs, RBAC).
  - The watcher graduates into a real "always-on" service users hit hourly (per ADR-023's evolution into a service).
  - A second frontend appears (mobile, embedded view inside another product).
  - User research shows 30%+ of session time is spent fighting Streamlit limitations (sidebar reload, no deep-links, no real-time).

The cleanest signal is the third: if anyone asks for an "embed this lineage view in our internal portal" feature, you cannot satisfy that with Streamlit, and that's the day to start the migration.

---

## Section 4: Streamlit-only Refactor — Phased Plan

Each phase is sized for ~1–3 commits (~3–5 days of focused work). Phases are ordered by impact-per-effort, not by code locality. Each phase ends with Sentinel review per `docs/crew.md`'s Layer 2 protocol.

### Phase A — Sidebar collapse + credentials modal (P0 unblock)

**Goal:** Cut the sidebar from 8+ expanders to ≤4 max; move credential entry off the sidebar entirely.

**Crew:**
- **Prism** — final UX spec for the new sidebar shape and the credentials dialog.
- **Blueprint** — confirm the credentials dialog fits the existing session-state shape (no plumbing changes downstream of the sidebar).
- **Weaver** — implement the modal + new sidebar layout.
- **Lens** — add tests for the credential dialog open/save round-trip via `streamlit.testing.v1.AppTest`.
- **Sentinel** — lint, format, tests, and a manual smoke check that no extract/push action regressed.

**Files touched:**
- `lineage_bridge/ui/sidebar/__init__.py` (sidebar composition)
- `lineage_bridge/ui/sidebar/credentials.py` (rewrite as `st.dialog`)
- `lineage_bridge/ui/sidebar/scope.py` (replace inline credential expanders with "Manage credentials" buttons)
- `lineage_bridge/ui/sidebar/actions.py` (collapse 4 publish render functions into a single "Publish" panel; merge Extract/Enrich/Refresh into a single primary + dropdown)
- `lineage_bridge/ui/sidebar/connection.py` (remove duplicate connection-status caption)

**Acceptance criteria:**
- Sidebar contains exactly 4 expanders: Setup, Run, Publish, Explore.
- Credentials dialog opens via "Manage credentials" button per env / cluster row; all per-env SR + Flink + per-cluster keys live there.
- Single primary "Run extraction" button with chevron-dropdown for Re-extract / Enrich / Refresh.
- Single "Publish" panel listing UC / Glue / DataZone / Google with per-target status pill.
- All existing extract / enrich / refresh / push flows pass `pytest tests/ -v` and a manual end-to-end smoke (extract → enrich → push UC → push Glue).
- Sidebar viewport height (with 2 envs / 3 clusters / connected) ≤ 1.2× viewport height (down from ~3–4×).

### Phase B — Graph canvas performance + interactions

**Goal:** Cache layout + render output; add double-click focus and search highlighting.

**Crew:**
- **Prism** — interaction spec (double-click semantics, search-highlight visual treatment).
- **Blueprint** — confirm cache key shape (frozenset of node_ids + edges hash) is correct and doesn't memory-leak on repeated re-extractions.
- **Weaver** — implement caching + `@st.cache_data` decorators + vis.js double-click handler.
- **Lens** — extend `tests/ui/test_graph_renderer.py` with cache-hit / cache-miss tests; integration test for the focus-on-double-click round-trip.
- **Sentinel** — review.

**Files touched:**
- `lineage_bridge/ui/graph_renderer.py` (`@st.cache_data` on `_compute_dag_layout` and `render_graph_raw`)
- `lineage_bridge/ui/components/visjs_graph/index.html` (add `network.on("doubleClick")` handler; add highlight-class for matched nodes)
- `lineage_bridge/ui/app.py` (handle the `{action: "focus", id}` payload from the iframe)
- `lineage_bridge/ui/sidebar/filters.py` (drive search-highlight by setting a flag on matched nodes)

**Acceptance criteria:**
- On a 500-node graph, switching a single filter checkbox completes in < 200ms (down from ~1s).
- Double-clicking a node sets `focus_node` and triggers a re-layout — no extra clicks needed.
- When a search query is active, matched nodes have a visible glow / wider border distinguishing them from neighbourhood-only nodes.
- All existing graph_renderer tests still pass; new cache tests cover invalidation when filters / search / focus change.

### Phase C — Logs, status, and watcher fixes

**Goal:** Stop losing the extraction log on push; surface watcher push results; fix the audit-log default trap.

**Crew:**
- **Prism** — final spec for the dual-log panel and the watcher event-feed redesign.
- **Blueprint** — confirm splitting `extraction_log` into two session-state lists doesn't break the orchestrator's progress callback (it doesn't — the callback is set per-action in `extraction.py:_ui_progress`).
- **Weaver** — implement `extraction_log` + `push_log` split, live-refresh fragment for the persisted log, REST-Polling default for watcher.
- **Lens** — add a parametrized test that runs extract → push → asserts both logs are populated and visible.
- **Sentinel** — review.

**Files touched:**
- `lineage_bridge/ui/sidebar/actions.py` (split log lists, drop `_log_source`, add live-refresh fragment around the log expander)
- `lineage_bridge/ui/extraction.py` (route progress to either `extraction_log` or `push_log` based on the action)
- `lineage_bridge/ui/watcher.py` (default mode REST Polling; gate Audit Log behind "Show advanced"; emit push-result events into `event_feed`)
- `lineage_bridge/ui/app.py` (add global status strip below header; tab badge for new watcher events)

**Acceptance criteria:**
- Run extraction, then push to UC: both Extraction Log and Push Log are populated and visible at the same time.
- Mid-extraction, the persisted log expander streams new lines (no stale-empty period).
- Watcher tab opens with REST Polling selected; Audit Log requires opening "Show advanced".
- When the watcher pushes to UC, a row appears in the watcher event feed within one fragment refresh cycle.
- Global status strip shows env name + cluster name + last extraction time and survives tab switches.
- All existing tests pass.

### Phase D — Detail panel polish + accessibility sweep

**Goal:** Detail panel rerun cost drops; status colour duplication ends; baseline ARIA coverage on custom HTML.

**Crew:**
- **Prism** — final spec for the new section ordering, the icon-on-neighbour-button design, and the deep-link-unavailable copy.
- **Weaver** — implement fragment-based panel, centralised status badge, key-introspection pattern for "Other Attributes", reorder, ARIA labels.
- **Lens** — extend `tests/ui/test_node_details.py` to cover each NodeType + `catalog_type` variant including the deep-link-unavailable case.
- **Sentinel** — review + accessibility checklist (axe-core spot check via browser devtools).

**Files touched:**
- `lineage_bridge/ui/node_details.py` (fragment wrap, single dismiss button, key-introspection inversion, section reorder, neighbour icons, ARIA on custom HTML)
- `lineage_bridge/ui/styles.py` (export a `render_status_badge_html(status)` helper that everyone uses; update `_known_keys` callers)
- `lineage_bridge/ui/sidebar/__init__.py` (ARIA on the connection / focus status badges)
- `lineage_bridge/ui/empty_state.py` (ARIA on step tracker, hero card)
- `lineage_bridge/ui/sidebar/filters.py` (ARIA on legend grid)

**Acceptance criteria:**
- Walking 5 nodes upstream via the Neighbours buttons completes in < 500ms total (down from ~3s) — fragment reruns instead of full reruns.
- Removing or renaming a status colour anywhere requires editing only `styles.py:STATUS_BADGE_MAP`. No other file references colour-by-state.
- Adding a new attribute to a NodeType client surfaces it in the type-specific section if explicitly handled, or in "Other Attributes" otherwise — never both.
- `axe-core` browser-devtools scan reports zero critical issues on the default extracted graph.
- All existing tests pass.

---

End of proposal. Ready for Blueprint review on Phase A scope before Weaver starts implementation.
