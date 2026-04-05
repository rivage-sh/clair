# PRD: `clair docs` -- Local Documentation Server

**Author:** Product Manager (agent)
**Date:** 2026-03-21
**Status:** Draft
**Audience:** Software architect and implementing engineers

---

## 1. Context and Motivation

clair users currently have two ways to understand their project's structure: reading Python files manually, or running `clair dag` to print an indented text tree. Neither is sufficient for projects with 50+ Trouves, where you need to quickly answer questions like:

- "What feeds into this table?"
- "Which columns does this Trouve expose?"
- "What tests protect this table?"
- "If I change this source, what breaks downstream?"

dbt solved this with `dbt docs serve` -- a local web UI that generates an interactive DAG and per-model documentation from project metadata. It is one of dbt's most-loved features and a key reason teams adopt it. clair needs the same capability.

**The job to be done:** A data engineer working on a clair project needs to explore, understand, and communicate the structure of their data pipeline without reading every Python file.

---

## 2. User Personas

### Primary: The Day-to-Day Data Engineer
Works in the clair project daily. Uses docs to trace lineage when debugging a broken table, to check what columns exist before writing a new Trouve, and to verify that tests cover critical tables.

### Secondary: The New Team Member
Just joined the team. Needs to build a mental model of the entire pipeline -- what layers exist, how data flows from sources to derived tables, and what each Trouve does.

### Tertiary: The Stakeholder / Analyst
Does not write clair code but needs to understand what tables are available, what they contain, and where the data comes from. Navigates the docs UI to find tables they can query.

---

## 3. Core Features (MVP)

### 3.1 CLI Command: `clair docs`

```bash
clair docs --project ./my_project [--port 8080] [--host 127.0.0.1] [--no-browser]
```

**Behavior:**
1. Discover the project (same as `clair compile` -- no Snowflake connection).
2. Build the DAG.
3. Extract metadata from every Trouve into a JSON catalog.
4. Start a local HTTP server serving a single-page web application.
5. Open the user's default browser (unless `--no-browser`).
6. Keep serving until Ctrl+C.

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--port` | `8741` | Port for the local server |
| `--host` | `127.0.0.1` | Bind address |
| `--no-browser` | `false` | Skip opening the browser |

Port 8741 chosen to avoid conflicts with common dev ports (3000, 5000, 8000, 8080). If the port is in use, the server should fail with a clear error message -- no silent fallback.

### 3.2 Interactive DAG Lineage Graph

The centerpiece of the UI. A visual, interactive directed graph where:

- Each node is a Trouve, visually distinguished by type (SOURCE, TABLE, VIEW).
- Edges show data flow (dependency -> dependent).
- Clicking a node highlights its upstream and downstream lineage and opens its detail panel.
- The graph is pannable and zoomable.
- Nodes are labeled with their short name (the table name), with the full `database.schema.table` name visible on hover or in the detail panel.
- Layout is left-to-right (sources on the left, derived on the right), following the natural flow of data.

**Node visual encoding:**
| TrouveType | Shape | Color rationale |
|------------|-------|-----------------|
| SOURCE | Rectangle | External data coming in |
| TABLE | Rounded rectangle | Materialized, "solid" |
| VIEW | Dashed-border rounded rectangle | Virtual, not materialized |

### 3.3 Searchable Trouve List (Sidebar)

A filterable list of all Trouves in the project, displayed as a sidebar alongside the graph. Supports:

- **Text search** on full_name, docs string, and column names.
- **Type filter** checkboxes: SOURCE, TABLE, VIEW.
- **Database/schema grouping**: Trouves grouped by their directory structure (database > schema), collapsible.

Clicking a Trouve in the list selects it on the graph and opens its detail panel.

### 3.4 Trouve Detail Panel

When a Trouve is selected (from graph or sidebar), a detail panel displays:

| Section | Content | Source |
|---------|---------|--------|
| **Header** | Full name (`database.schema.table`), TrouveType badge, docs string | `trouve.full_name`, `trouve.type`, `trouve.docs` |
| **Columns** | Table of column name, type, nullable, column docs | `trouve.columns[]` |
| **SQL** | Syntax-highlighted resolved SQL | `trouve.compiled.resolved_sql` |
| **Tests** | List of attached tests with type and parameters | `trouve.tests[]` |
| **Run Config** | RunMode, IncrementalMode, unique_key (if incremental) | `trouve.run_config` |
| **Lineage** | Lists of direct upstream and downstream Trouve names (clickable links) | DAG edges |
| **File path** | Relative path to the `.py` file | `trouve.compiled.file_path` relative to project root |

For SOURCE Trouves, the SQL section is omitted (they have no SQL). The Run Config section is omitted for VIEWs and SOURCEs.

### 3.5 Catalog JSON Generation

Before serving the UI, clair generates a JSON document (the "catalog") that contains all metadata the frontend needs. This is the contract between the Python backend and the JavaScript frontend.

```json
{
  "project_name": "my_project",
  "generated_at": "2026-03-21T14:30:00Z",
  "clair_version": "0.1.0",
  "trouves": {
    "source.products.catalog": {
      "full_name": "source.products.catalog",
      "database": "source",
      "schema": "products",
      "name": "catalog",
      "type": "source",
      "docs": "Raw product catalog from the ERP system.",
      "columns": [
        {"name": "product_id", "type": "STRING", "docs": "", "nullable": true},
        {"name": "name", "type": "STRING", "docs": "", "nullable": true}
      ],
      "tests": [],
      "sql": null,
      "run_config": {
        "run_mode": "full_refresh",
        "incremental_mode": null,
        "unique_key": null
      },
      "file_path": "source/products/catalog.py",
      "upstream": [],
      "downstream": ["refined.products.catalog"]
    }
  },
  "edges": [
    {"from": "source.products.catalog", "to": "refined.products.catalog"}
  ]
}
```

The catalog is generated in-memory and served by the HTTP server at `/api/catalog.json`. It is NOT written to disk (no _clairtifacts pollution). The frontend fetches it on page load.

---

## 4. Key UX Flows

### Flow 1: "What feeds into this table?"
1. User runs `clair docs --project ./my_project`.
2. Browser opens. User sees the full DAG graph.
3. User clicks on `derived.products.top_reviewed` in the graph.
4. The graph highlights all upstream nodes in a distinct color. Non-lineage nodes are dimmed.
5. The detail panel shows the upstream list: `refined.products.catalog`, `refined.products.reviews`.
6. User clicks `refined.products.catalog` in the upstream list to trace further.

### Flow 2: "What columns does this table have?"
1. User types "catalog" in the search bar.
2. Sidebar filters to show `source.products.catalog` and `refined.products.catalog`.
3. User clicks `refined.products.catalog`.
4. Detail panel shows the Columns section (if columns are defined) or shows "No columns defined" with a note that adding `columns=[...]` to the Trouve will populate this section.

### Flow 3: "What tests protect this pipeline?"
1. User scans the DAG. Nodes with tests show a small badge/indicator (e.g., a checkmark icon or a count).
2. User clicks a node with tests. The Tests section in the detail panel lists each test: `unique(event_id)`, `not_null(user_id)`, `row_count(min=1)`.
3. Nodes without tests have no badge, making coverage gaps visually obvious.

### Flow 4: "Show me just the derived layer"
1. User unchecks SOURCE and TABLE in the type filter.
2. The sidebar shows only VIEWs. The graph dims or hides non-matching nodes.
3. User re-checks all types to return to the full view.

---

## 5. Data Model: What Gets Extracted

The catalog is built entirely from data already available after `discover_project()` and `build_dag()`. No new data collection is needed. Here is the mapping:

| Catalog field | Source |
|---------------|--------|
| `full_name` | `trouve.compiled.full_name` |
| `database`, `schema`, `name` | Split `full_name` on `.` |
| `type` | `trouve.type.value` |
| `docs` | `trouve.docs` |
| `columns` | `trouve.columns` (list of Column pydantic models) |
| `tests` | `trouve.tests` (list of Test subclass instances -- serialize type + params) |
| `sql` | `trouve.compiled.resolved_sql` (None for SOURCE) |
| `run_config` | `trouve.run_config` (RunMode, IncrementalMode, unique_key) |
| `file_path` | `trouve.compiled.file_path` relative to project root |
| `upstream` | `list(dag.predecessors(full_name))` |
| `downstream` | `list(dag.successors(full_name))` |
| `edges` | `list(dag.edges)` |

Every field above is already populated by the existing discovery + DAG pipeline. The docs command adds zero new data sources.

---

## 6. Clair-Specific Considerations

### 6.1 Python Imports as Lineage
Unlike dbt (which uses `ref()` / `source()` macros), clair infers lineage from Python imports. The docs UI should show the import-based lineage as-is. No special handling is needed -- the DAG edges are already resolved by discovery.

### 6.2 TrouveType Distinctions
SOURCEs are fundamentally different from TABLEs/VIEWs:
- No SQL to display.
- No run config.
- They represent external data, not clair-managed transformations.

The UI must treat SOURCEs as "inputs to the system" visually and informationally. They should appear on the leftmost edge of the DAG graph and have a distinct visual treatment.

### 6.3 Three-Level Naming (database.schema.table)
clair's naming maps 1:1 to Snowflake's three-part naming. The sidebar grouping should mirror this: database > schema > table. This is more structured than dbt's flat model list.

### 6.4 Incremental Config Visibility
Users need to see at a glance whether a Trouve is incremental (APPEND or UPSERT) or full-refresh. This is important for understanding pipeline behavior. The detail panel should display this clearly.

### 6.5 Column Documentation Gaps
Many Trouves will not have `columns` defined (it is optional for TABLE/VIEW types -- only required for UPSERT). The UI should handle this gracefully: show "No columns defined" rather than an empty table, and ideally nudge the user to add column definitions.

### 6.6 Test Coverage Visibility
One of the highest-value features of a docs UI is making test coverage gaps visible. The DAG should visually distinguish Trouves with tests from those without.

---

## 7. Non-Goals (Out of Scope for MVP)

- **Live Snowflake connection**: The docs server never connects to Snowflake. All metadata comes from the Python source files.
- **Row counts or data profiling**: No runtime statistics. This is a structural documentation tool.
- **Editing Trouves from the UI**: Read-only.
- **Multi-project support**: One project per server instance.
- **Persisted catalog to disk**: The catalog is generated on server start and served from memory. No `clair docs generate` step.
- **Hot reload / file watching**: If the user changes a Trouve file, they must restart the server. File watching is a nice-to-have for later.
- **Authentication / sharing**: The server binds to localhost. No auth needed.
- **Full-text search of SQL content**: Search covers full_name, docs, and column names, not SQL body.

---

## 8. Nice-to-Haves (Post-MVP)

Ordered by estimated user value:

1. **File watching + auto-reload**: Watch `.py` files in the project, re-run discovery, push updated catalog to the frontend via WebSocket. Eliminates the restart-to-see-changes friction.
2. **Export DAG as image**: Download the current graph view as PNG or SVG for use in documentation or presentations.
3. **Keyboard navigation**: Arrow keys to traverse the graph, `/` to focus search, `Esc` to deselect.
4. **SQL search**: Include SQL content in the search index.
5. **`clair docs generate`**: Write the catalog JSON + static HTML to a directory for hosting on a static file server (e.g., GitHub Pages) without running the local server.
6. **Deep linking**: URL fragments like `#/trouve/derived.products.top_reviewed` so users can share links to specific Trouves.
7. **Dark mode**.

---

## 9. Technical Constraints and Guidance

These are decisions and constraints for the architect to work within:

1. **The frontend must be a static SPA bundled into the Python package.** No Node.js runtime dependency. The Python server serves pre-built HTML/JS/CSS. The architect should choose a lightweight JS framework (or vanilla JS) that can be bundled as static assets in `src/clair/docs/static/`.

2. **The Python server should use the stdlib.** `http.server` or a minimal ASGI/WSGI library already in the dependency tree. Do not add Flask, FastAPI, or any web framework as a dependency. The server has exactly two routes: serve the SPA (HTML/JS/CSS) and serve `/api/catalog.json`.

3. **Graph rendering library.** The architect should evaluate lightweight DAG visualization options that work in the browser without heavy dependencies. Options to consider: D3.js (manual but flexible), Cytoscape.js (purpose-built for graph viz), ELK.js (layered layout algorithm). The key requirement is a left-to-right hierarchical layout that handles 50-200 nodes without performance issues.

4. **The catalog JSON is the API contract.** The frontend and backend communicate only through this JSON blob. No server-side rendering, no templates.

5. **Existing infrastructure to reuse:**
   - `discover_project()` and `build_dag()` already do all the heavy lifting.
   - `lineage.get_dag()` is the public API that wraps both.
   - `ClairDag` (networkx DiGraph subclass) provides `.predecessors()`, `.successors()`, `.edges`, and `.nodes` with Trouve attributes.
   - All Trouve/Column/Test models are Pydantic and can be serialized to JSON with `.model_dump()`.

6. **Dependency budget:** Keep new dependencies minimal. The graph rendering library (JS, bundled as static assets) adds no Python dependency. If a Python dependency is needed for the HTTP server beyond stdlib, justify it.

---

## 10. Success Criteria

The MVP is complete when:

- [ ] `clair docs --project <path>` starts a local server and opens the browser.
- [ ] The DAG graph renders all Trouves with correct edges and type-based visual styling.
- [ ] Clicking a node shows its detail panel with all sections (header, columns, SQL, tests, lineage, file path).
- [ ] The sidebar lists all Trouves grouped by database/schema with text search.
- [ ] Type filter checkboxes work to show/hide SOURCE/TABLE/VIEW.
- [ ] The graph supports pan, zoom, and click-to-highlight-lineage.
- [ ] Works on `example_2` (50-Trouve project) without noticeable lag.
- [ ] No Snowflake connection required.
- [ ] No new Python runtime dependencies beyond stdlib (JS is bundled as static assets).

---

## 11. Open Questions for the Architect

1. **Graph library choice**: D3.js vs Cytoscape.js vs ELK.js -- what gives the best out-of-the-box hierarchical layout with the least custom code?
2. **Static asset bundling**: How should the frontend assets be included in the Python package? Checked into `src/clair/docs/static/`? Built as part of the Python package build step?
3. **HTTP server choice**: Is `http.server` from stdlib sufficient, or should we use something slightly more capable? The server is trivial (two routes) so this should be simple.
4. **Catalog generation performance**: For a 200-Trouve project, is in-memory catalog generation fast enough to be imperceptible on server start? (Almost certainly yes, since discovery is already fast, but worth confirming.)
