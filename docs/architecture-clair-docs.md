# Architecture: `clair docs`

**Author:** Software Architect (agent)
**Date:** 2026-03-21
**Status:** Ready for implementation
**Input:** [PRD: clair docs](./prd-clair-docs.md)

---

## 1. New File Structure

```
src/clair/docs/
    __init__.py          # Package marker; exports build_catalog, serve
    catalog.py           # build_catalog(dag, project_root) -> dict (uses Trouve.model_dump)
    server.py            # CatalogHandler, serve(catalog, host, port, open_browser)
    static/
        index.html       # SPA shell: three-panel layout, all CSS inline in <style>
        app.js           # Application logic: fetch catalog, render sidebar, detail panel, wire events
        graph.js         # Cytoscape.js initialization, node/edge schema, layout config, highlight logic
```

**Total: 4 new Python files, 3 new static files (1 HTML, 2 JS). No vendored JS.**

JS dependencies (Cytoscape.js, dagre, cytoscape-dagre) are loaded from pinned CDN URLs at runtime. See section 4.7 for details.

No new test fixture files are needed -- existing `simple_project` fixture is sufficient for catalog generation tests.

---

## 2. Python Backend Design

### 2.1 `src/clair/docs/__init__.py`

```python
"""clair docs -- local documentation server for clair projects."""

from clair.docs.catalog import build_catalog
from clair.docs.server import serve

__all__ = ["build_catalog", "serve"]
```

### 2.2 `src/clair/docs/catalog.py`

**Purpose:** Build the catalog dict that the frontend consumes. There are no custom Pydantic models -- `Trouve.model_dump(mode="json")` already produces the complete per-node payload. The catalog is a plain dict, not a Pydantic model.

#### `build_catalog` Function

```python
"""Catalog builder for clair docs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from clair import __version__
from clair.core.dag import ClairDag


def build_catalog(dag: ClairDag, project_root: Path) -> dict:
    """Build a catalog dict from a compiled DAG.

    Args:
        dag: A validated ClairDag (from build_dag).
        project_root: Absolute path to the project root.

    Returns:
        A JSON-serializable dict. The server serializes this to bytes.
    """
    return {
        "project_name": project_root.name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clair_version": __version__,
        "trouves": {fn: dag.get_trouve(fn).model_dump(mode="json") for fn in dag.nodes},
        "edges": [{"source": source, "target": target} for source, target in dag.edges],
    }
```

That is the entire module. No classes, no post-processing, no field remapping.

**Why this works:**

- `Trouve.model_dump(mode="json")` serializes all fields the frontend needs: `full_name`, `type`, `docs`, `columns`, `tests`, `sql` (via `compiled.resolved_sql`), `run_config`, and `file_path` (already stored as a project-relative string in `CompiledAttributes`).
- The frontend receives a flat `edges` list and computes upstream/downstream relationships itself using Cytoscape.js graph traversal (`incomers()` / `outgoers()`). This is exactly what a graph library is for -- duplicating adjacency data in the payload would be redundant.
- `tests.length` is trivial to compute in JS; there is no need for a denormalized `test_count` field.

**Design decisions:**

- No Pydantic catalog models. The Trouve/Column/Test/RunConfig models already define the schema. Adding a parallel set of "catalog" models would create a maintenance burden with no benefit -- every field change on Trouve would require a corresponding change in the catalog model.
- The catalog is a plain `dict`, not a Pydantic model. There is nothing to validate at construction time (the DAG is already validated), and `json.dumps(dict)` is just as good as `model.model_dump(mode="json")` when the dict is already JSON-serializable.
- `file_path` is stored as a project-relative POSIX string in `CompiledAttributes` (set during discovery), so no path relativization is needed here.

### 2.3 `src/clair/docs/server.py`

**Purpose:** Serve the SPA and catalog JSON using stdlib `http.server`.

```python
"""Local HTTP server for clair docs."""

from __future__ import annotations

import json
import mimetypes
import threading
import webbrowser
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import structlog

logger = structlog.get_logger()

STATIC_DIR = Path(__file__).parent / "static"


class CatalogHandler(SimpleHTTPRequestHandler):
    """Serves static files from STATIC_DIR and the catalog at /api/catalog.json.

    The catalog bytes are attached to the server instance as `server.catalog_json`
    (a pre-serialized bytes object) to avoid re-serializing on every request.
    """

    def do_GET(self) -> None:
        if self.path == "/api/catalog.json":
            self._serve_catalog()
        elif self.path == "/" or not self._static_file_exists():
            # SPA fallback: serve index.html for any path that doesn't
            # match a static file. This supports potential future deep linking.
            self._serve_file("index.html")
        else:
            self._serve_file(self.path.lstrip("/"))

    def _serve_catalog(self) -> None:
        body = self.server.catalog_json  # type: ignore[attr-defined]
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, relative_path: str) -> None:
        file_path = STATIC_DIR / relative_path
        if not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = file_path.read_bytes()
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _static_file_exists(self) -> bool:
        """Check if the request path maps to an actual file in STATIC_DIR."""
        candidate = STATIC_DIR / self.path.lstrip("/")
        # Prevent directory traversal
        try:
            candidate.resolve().relative_to(STATIC_DIR.resolve())
        except ValueError:
            return False
        return candidate.is_file()

    def log_message(self, format: str, *args) -> None:
        """Suppress default stderr logging -- we use structlog."""
        pass


class CatalogServer(HTTPServer):
    """HTTPServer subclass that carries the pre-serialized catalog."""

    catalog_json: bytes


def serve(
    catalog: dict,
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None:
    """Start the docs server. Blocks until Ctrl+C.

    Args:
        catalog: The catalog dict from build_catalog().
        host: Bind address.
        port: Bind port.
        open_browser: Whether to open the user's default browser.
    """
    catalog_bytes = json.dumps(
        catalog, separators=(",", ":")
    ).encode("utf-8")

    server = CatalogServer((host, port), CatalogHandler)
    server.catalog_json = catalog_bytes

    url = f"http://{host}:{port}"
    logger.info("docs.serving", url=url)

    if open_browser:
        # Open in a thread so it doesn't delay the server start
        threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        logger.info("docs.stopped")
```

**Key design decisions:**

1. **Pre-serialized catalog**: `json.dumps(catalog)` is called once at startup. The resulting bytes are served from memory on every request. No per-request serialization overhead.

2. **SPA fallback**: Any URL that doesn't match a static file serves `index.html`. This is forward-compatible with deep linking (`#/trouve/...` or path-based routes).

3. **Directory traversal protection**: `_static_file_exists` validates that resolved paths stay within `STATIC_DIR`.

4. **No threading model for requests**: `HTTPServer` handles one request at a time. This is fine for a local single-user dev tool. The catalog is read-only and the static files are tiny.

5. **Graceful shutdown**: `KeyboardInterrupt` triggers `server_close()`. The `serve_forever()` loop is the blocking call.

6. **Suppressed access logs**: The default `SimpleHTTPRequestHandler.log_message` prints to stderr. We suppress it to keep the terminal clean (structlog handles our logging).

### 2.4 CLI Integration in `src/clair/cli/main.py`

Add a new `docs` command to the existing `cli` group. Place it after the `dag` command (alphabetical neighbor).

```python
@cli.command()
@click.option(
    "--project",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="Path to the Clair project root",
)
@click.option(
    "--port",
    default=8741,
    type=int,
    help="Port for the local docs server",
)
@click.option(
    "--host",
    default="127.0.0.1",
    help="Bind address for the local docs server",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Do not open the browser automatically",
)
def docs(project: str, port: int, host: str, no_browser: bool) -> None:
    """Start a local web UI showing project documentation and lineage."""
    project_root = Path(project).resolve()

    try:
        discovered = discover_project(project_root)
        dag = build_dag(discovered)

        from clair.docs.catalog import build_catalog
        from clair.docs.server import serve

        catalog = build_catalog(dag, project_root)

        source_count = sum(1 for t in dag.trouves if t.type == TrouveType.SOURCE)
        trouve_count = len(dag.nodes) - source_count

        logger.info("docs.start", project=str(project_root), trouves=trouve_count, sources=source_count)

        serve(catalog, host=host, port=port, open_browser=not no_browser)

    except OSError as e:
        if "Address already in use" in str(e) or "address already in use" in str(e):
            logger.error("docs.port_in_use", port=port, detail=f"Port {port} is already in use. Try --port <other>")
        else:
            logger.error("docs.error", error=str(e))
        sys.exit(1)
    except ClairError as e:
        logger.error("docs.error", error=str(e))
        sys.exit(1)
```

**Notes:**
- Imports `clair.docs.*` lazily inside the function body to avoid loading the docs module (and its static file I/O) for other commands.
- Uses the same `discover_project` + `build_dag` pattern as `compile` and `dag` commands.
- Catches `OSError` specifically for the port-in-use case (stdlib `HTTPServer` raises `OSError: [Errno 48] Address already in use`).
- No `--select` flag. The docs UI shows the full project. Filtering is done in the browser.

---

## 3. Catalog JSON Shape

The catalog is a plain dict. The per-Trouve payload is exactly `Trouve.model_dump(mode="json")` -- its shape is defined by the existing Pydantic models (`Trouve`, `CompiledAttributes`, `Column`, test types, `RunConfig`). No custom schema definition is needed.

**Top-level keys:**

| Key | Type | Description |
|-----|------|-------------|
| `project_name` | `string` | `project_root.name` |
| `generated_at` | `string` | ISO 8601 UTC timestamp |
| `clair_version` | `string` | `clair.__version__` |
| `trouves` | `dict[string, object]` | Keyed by `full_name`; each value is `Trouve.model_dump(mode="json")` |
| `edges` | `list[object]` | Each element is `{"source": full_name, "target": full_name}` |

**Invariants:**
- `trouves` keys always equal the `full_name` field inside the value.
- `columns` and `tests` are always arrays, never null.
- `sql` (inside `compiled`) is null for SOURCE type.
- `run_config` is null for SOURCE and VIEW types.
- Every edge's `source`/`target` exists as a key in `trouves`.
- Upstream/downstream relationships are NOT stored per node -- the frontend derives them from `edges` using Cytoscape.js graph traversal.

---

## 4. Frontend Design

### 4.1 Asset Structure

Three files, loaded alongside CDN-hosted JS libraries. NOT all inline -- separate files are easier to debug, cache, and maintain.

| File | Purpose | Approximate size |
|------|---------|-----------------|
| `index.html` | HTML shell + inline `<style>` for all CSS | ~8KB |
| `app.js` | Sidebar, detail panel, search, filters, event wiring | ~15KB |
| `graph.js` | Cytoscape init, layout, highlight, pan/zoom | ~10KB |

**JS dependencies loaded from CDN (pinned versions):**

| Library | Version | CDN URL |
|---------|---------|---------|
| Cytoscape.js | 3.30.2 | `https://cdn.jsdelivr.net/npm/cytoscape@3.30.2/dist/cytoscape.min.js` |
| dagre | 0.8.5 | `https://cdn.jsdelivr.net/npm/dagre@0.8.5/dist/dagre.min.js` |
| cytoscape-dagre | 2.5.0 | `https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js` |

The app requires internet at runtime to load these libraries. This is acceptable for a local dev tool.

**CSS is inline in `index.html`** (single `<style>` block). The styling is simple enough that a separate CSS file adds complexity without benefit. The JS files are loaded as standard `<script>` tags (not ES modules) to keep the loading simple.

### 4.2 Layout: Three-Panel Design

```
+------------+---------------------------+------------------+
|  SIDEBAR   |       GRAPH PANEL         |  DETAIL PANEL    |
|  250px     |       flex: 1             |  350px           |
|            |                           |                  |
| [search]   |                           |  (hidden until   |
| [filters]  |    [cytoscape canvas]     |   a node is      |
| [tree]     |                           |   selected)      |
|            |                           |                  |
+------------+---------------------------+------------------+
```

**CSS approach: Flexbox**

```css
body {
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    height: 100vh;
    overflow: hidden;
}

.app {
    display: flex;
    height: 100vh;
}

.sidebar {
    width: 250px;
    min-width: 250px;
    border-right: 1px solid #e0e0e0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.sidebar-header {
    padding: 12px;
    border-bottom: 1px solid #e0e0e0;
}

.sidebar-tree {
    flex: 1;
    overflow-y: auto;
    padding: 8px 0;
}

.graph-panel {
    flex: 1;
    position: relative;
    background: #fafafa;
}

.detail-panel {
    width: 350px;
    min-width: 350px;
    border-left: 1px solid #e0e0e0;
    overflow-y: auto;
    display: none;          /* shown when a node is selected */
}

.detail-panel.open {
    display: block;
}
```

### 4.3 Cytoscape.js Node/Edge Schema

**Node data:**

```javascript
{
    group: "nodes",
    data: {
        id: "source.products.catalog",         // full_name, used as unique ID
        label: "catalog",                       // short name (table_name segment)
        fullName: "source.products.catalog",    // for display
        type: "source",                         // "source" | "table" | "view"
        databaseName: "source",
        schemaName: "products",
        testCount: 0,                           // computed from tests.length
        hasDocs: true,                          // docs !== ""
    }
}
```

**Edge data:**

```javascript
{
    group: "edges",
    data: {
        id: "source.products.catalog->refined.products.catalog",  // unique edge ID
        source: "source.products.catalog",     // Cytoscape uses "source" for from-node
        target: "refined.products.catalog",    // Cytoscape uses "target" for to-node
    }
}
```

**Stylesheet (Cytoscape style array):**

```javascript
const style = [
    // --- Default node ---
    {
        selector: "node",
        style: {
            "label": "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "font-size": "11px",
            "font-family": "-apple-system, BlinkMacSystemFont, sans-serif",
            "background-color": "#4a90d9",
            "color": "#fff",
            "width": "label",
            "height": 32,
            "padding": "8px",
            "shape": "roundrectangle",
            "text-wrap": "none",
        }
    },
    // --- SOURCE nodes: rectangle, distinct color ---
    {
        selector: "node[type='source']",
        style: {
            "background-color": "#6b7280",     // gray -- external/input
            "shape": "rectangle",
        }
    },
    // --- TABLE nodes: rounded rectangle, blue ---
    {
        selector: "node[type='table']",
        style: {
            "background-color": "#4a90d9",     // blue -- materialized
            "shape": "roundrectangle",
        }
    },
    // --- VIEW nodes: dashed border, lighter fill ---
    {
        selector: "node[type='view']",
        style: {
            "background-color": "#93c5fd",     // light blue -- virtual
            "border-style": "dashed",
            "border-width": 2,
            "border-color": "#4a90d9",
        }
    },
    // --- Nodes with tests: small badge indicator ---
    {
        selector: "node[testCount > 0]",
        style: {
            "border-width": 2,
            "border-color": "#22c55e",         // green border = has tests
        }
    },
    // --- Default edge ---
    {
        selector: "edge",
        style: {
            "width": 1.5,
            "line-color": "#cbd5e1",
            "target-arrow-color": "#cbd5e1",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            "arrow-scale": 0.8,
        }
    },
    // --- Highlighted (lineage) nodes ---
    {
        selector: "node.highlighted",
        style: {
            "border-width": 3,
            "border-color": "#f59e0b",          // amber highlight
            "background-opacity": 1,
        }
    },
    // --- Selected node ---
    {
        selector: "node.selected-node",
        style: {
            "border-width": 3,
            "border-color": "#ef4444",           // red border for selected
        }
    },
    // --- Highlighted edges ---
    {
        selector: "edge.highlighted",
        style: {
            "line-color": "#f59e0b",
            "target-arrow-color": "#f59e0b",
            "width": 2.5,
        }
    },
    // --- Dimmed (non-lineage) elements ---
    {
        selector: "node.dimmed",
        style: {
            "opacity": 0.2,
        }
    },
    {
        selector: "edge.dimmed",
        style: {
            "opacity": 0.15,
        }
    },
];
```

**Layout configuration:**

```javascript
const layout = {
    name: "dagre",
    rankDir: "LR",           // left-to-right
    nodeSep: 40,             // vertical spacing between nodes in same rank
    rankSep: 80,             // horizontal spacing between ranks
    edgeSep: 10,
    padding: 30,
    animate: false,          // no animation on initial render
    fit: true,               // fit the graph to the viewport
};
```

### 4.4 Interaction Design

#### Click to Select

When a node is clicked (`cy.on("tap", "node", ...)`):

1. **Clear previous selection**: Remove `selected-node`, `highlighted`, `dimmed` classes from all elements.
2. **Mark selected**: Add `selected-node` class to the clicked node.
3. **Compute lineage**: Use BFS/DFS to find all upstream ancestors and downstream descendants.
   ```javascript
   function getLineage(cy, nodeId) {
       const upstream = new Set();
       const downstream = new Set();

       // Walk upstream (predecessors)
       const upQueue = [nodeId];
       while (upQueue.length) {
           const current = upQueue.pop();
           cy.getElementById(current).incomers("node").forEach(n => {
               if (!upstream.has(n.id())) {
                   upstream.add(n.id());
                   upQueue.push(n.id());
               }
           });
       }

       // Walk downstream (successors)
       const downQueue = [nodeId];
       while (downQueue.length) {
           const current = downQueue.pop();
           cy.getElementById(current).outgoers("node").forEach(n => {
               if (!downstream.has(n.id())) {
                   downstream.add(n.id());
                   downQueue.push(n.id());
               }
           });
       }

       return { upstream, downstream };
   }
   ```
4. **Apply classes**:
   - Add `highlighted` to all upstream and downstream nodes.
   - Add `highlighted` to all edges connecting lineage nodes.
   - Add `dimmed` to all nodes and edges NOT in the lineage set (and not the selected node).
5. **Open detail panel**: Populate and show the detail panel for the clicked node.

#### Click Background to Deselect

`cy.on("tap", function(e) { if (e.target === cy) { clearSelection(); } })` -- clears all highlight classes and hides the detail panel.

#### Sidebar Click

Clicking a Trouve name in the sidebar triggers the same selection logic as a graph click. Additionally, the graph pans to center the selected node: `cy.animate({ center: { eles: node }, duration: 300 })`.

#### Search

The search input fires on `input` event (live filtering, no submit button). It filters the sidebar tree and optionally dims non-matching nodes on the graph.

```javascript
function filterSidebar(query) {
    const q = query.toLowerCase();
    document.querySelectorAll(".sidebar-item").forEach(el => {
        const fullName = el.dataset.fullName;
        const docs = el.dataset.docs;
        const columns = el.dataset.columns;  // comma-joined column names
        const match = fullName.includes(q) || docs.includes(q) || columns.includes(q);
        el.style.display = match ? "" : "none";
    });
    // Also show/hide parent group headers if all children are hidden
    updateGroupVisibility();
}
```

#### Type Filters

Three checkboxes: SOURCE, TABLE, VIEW. All checked by default. On change:

1. Update sidebar visibility (same as search, combined with search query).
2. On the graph: toggle `display: element` / `display: none` on nodes of the unchecked type (and their edges that connect two hidden nodes).

```javascript
function applyTypeFilters(activeTypes) {
    cy.nodes().forEach(node => {
        if (activeTypes.has(node.data("type"))) {
            node.style("display", "element");
        } else {
            node.style("display", "none");
        }
    });
    // Edges auto-hide when both endpoints are hidden (Cytoscape default behavior)
}
```

### 4.5 Detail Panel Sections

The detail panel is a scrollable column. Sections are rendered conditionally.

#### Header (always shown)

```html
<div class="detail-header">
    <span class="type-badge type-{type}">{TYPE}</span>
    <h2>{full_name}</h2>
    <p class="docs">{docs or "No description."}</p>
</div>
```

The type badge uses the same color as the graph node. `TYPE` is uppercased for display.

#### Columns (always shown, even if empty)

If `columns.length === 0`:
```html
<div class="detail-section">
    <h3>Columns</h3>
    <p class="empty-state">No columns defined. Add <code>columns=[...]</code> to this Trouve.</p>
</div>
```

If columns exist:
```html
<div class="detail-section">
    <h3>Columns</h3>
    <table class="columns-table">
        <thead><tr><th>Name</th><th>Type</th><th>Nullable</th><th>Description</th></tr></thead>
        <tbody>
            <tr><td>product_id</td><td>STRING</td><td>Yes</td><td></td></tr>
            ...
        </tbody>
    </table>
</div>
```

#### SQL (TABLE and VIEW only, omitted for SOURCE)

```html
<div class="detail-section">
    <h3>SQL</h3>
    <pre class="sql-block"><code>{sql}</code></pre>
</div>
```

No syntax highlighting library. The `<pre>` block uses a monospace font with a light gray background. SQL is readable enough without highlighting, and adding a highlighter would mean another CDN dependency. This can be added post-MVP.

#### Tests (always shown)

If `tests.length === 0`:
```html
<div class="detail-section">
    <h3>Tests</h3>
    <p class="empty-state">No tests defined.</p>
</div>
```

If tests exist:
```html
<div class="detail-section">
    <h3>Tests</h3>
    <ul class="tests-list">
        <li><span class="test-type">unique</span> column: product_id</li>
        <li><span class="test-type">row_count</span> min_rows: 1</li>
    </ul>
</div>
```

Each test renders as `{label}({params formatted as key: value})`.

#### Run Config (TABLE only, omitted for SOURCE and VIEW)

```html
<div class="detail-section">
    <h3>Run Config</h3>
    <dl>
        <dt>Run Mode</dt><dd>full_refresh</dd>
        <dt>Incremental Mode</dt><dd>--</dd>
        <dt>Unique Key</dt><dd>--</dd>
    </dl>
</div>
```

Use `"--"` for null values.

#### Lineage (always shown)

Upstream and downstream lists are NOT in the catalog payload. The detail panel computes them at render time by querying the Cytoscape graph instance:

```javascript
// Direct predecessors/successors (one hop), not full transitive lineage
const upstream = ClairdocsGraph.getDirectPredecessors(fullName);
const downstream = ClairdocsGraph.getDirectSuccessors(fullName);
```

```html
<div class="detail-section">
    <h3>Upstream</h3>
    <ul class="lineage-list">
        <li><a href="#" data-trouve="source.products.catalog">source.products.catalog</a></li>
    </ul>
    <h3>Downstream</h3>
    <ul class="lineage-list">
        <li><a href="#" data-trouve="derived.products.summary">derived.products.summary</a></li>
    </ul>
</div>
```

If no upstream/downstream: `<p class="empty-state">None</p>`.

Clicking a lineage link triggers the same selection behavior as clicking a graph node (reuse the selection function).

#### File Path (always shown, last section)

```html
<div class="detail-section">
    <h3>File</h3>
    <code class="file-path">{file_path}</code>
</div>
```

### 4.6 Sidebar Tree Structure

The sidebar groups Trouves by `database_name > schema_name`. Each group is collapsible.

```html
<div class="sidebar-group" data-database="source">
    <div class="group-header" onclick="toggleGroup(this)">
        <span class="caret">&#9656;</span> source
    </div>
    <div class="group-children">
        <div class="sidebar-subgroup" data-schema="products">
            <div class="subgroup-header" onclick="toggleGroup(this)">
                <span class="caret">&#9656;</span> products
            </div>
            <div class="group-children">
                <div class="sidebar-item" data-full-name="source.products.catalog"
                     data-docs="raw product catalog" data-columns="product_id,name">
                    <span class="item-type-dot type-source"></span>
                    catalog
                </div>
            </div>
        </div>
    </div>
</div>
```

Each sidebar item has a small colored dot matching its type (source=gray, table=blue, view=light blue). The `data-*` attributes on `.sidebar-item` are used by the search filter.

### 4.7 `index.html` Structure

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>clair docs</title>
    <style>
        /* All CSS here -- see section 4.2 for the layout CSS */
        /* Plus: typography, colors, sidebar items, detail panel sections,
           search input, filter checkboxes, badges, tables, code blocks */
    </style>
</head>
<body>
    <div class="app">
        <aside class="sidebar">
            <div class="sidebar-header">
                <h1 class="logo">clair</h1>
                <input type="text" class="search-input" id="search" placeholder="Search trouves...">
                <div class="type-filters" id="type-filters">
                    <label><input type="checkbox" value="source" checked> Source</label>
                    <label><input type="checkbox" value="table" checked> Table</label>
                    <label><input type="checkbox" value="view" checked> View</label>
                </div>
            </div>
            <div class="sidebar-tree" id="sidebar-tree">
                <!-- Populated by app.js -->
            </div>
        </aside>
        <main class="graph-panel" id="graph-container">
            <!-- Cytoscape.js renders here -->
        </main>
        <aside class="detail-panel" id="detail-panel">
            <!-- Populated by app.js on node selection -->
        </aside>
    </div>

    <!-- JS dependencies: pinned CDN versions (requires internet) -->
    <script src="https://cdn.jsdelivr.net/npm/dagre@0.8.5/dist/dagre.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/cytoscape@3.30.2/dist/cytoscape.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
    <!-- Application JS -->
    <script src="graph.js"></script>
    <script src="app.js"></script>
</body>
</html>
```

**Script load order matters:**
1. `dagre` first (dependency of cytoscape-dagre)
2. `cytoscape` (core library)
3. `cytoscape-dagre` (registers the dagre layout with cytoscape)
4. `graph.js` (our graph module, depends on cytoscape)
5. `app.js` (our app module, depends on graph.js)

### 4.8 `app.js` Module Design

`app.js` is the application orchestrator. It owns the catalog data, the sidebar, the detail panel, and the event wiring. It delegates graph rendering to `graph.js`.

```javascript
// app.js -- Application logic for clair docs

(function() {
    "use strict";

    let catalog = null;    // set after fetch
    let selectedId = null; // currently selected node's full_name

    // --- Bootstrap ---
    document.addEventListener("DOMContentLoaded", async function() {
        const response = await fetch("/api/catalog.json");
        catalog = await response.json();
        document.querySelector(".logo").textContent = "clair -- " + catalog.project_name;
        renderSidebar(catalog.trouves);
        ClairdocsGraph.init("graph-container", catalog, onNodeSelect);
        wireSearch();
        wireTypeFilters();
    });

    // --- Sidebar rendering ---
    function renderSidebar(trouves) { /* build grouped tree from trouves */ }

    // --- Detail panel ---
    function showDetail(trouveData) { /* render all sections into #detail-panel */ }
    function hideDetail() { /* hide #detail-panel, clear selectedId */ }

    // --- Selection logic (called from graph click or sidebar click) ---
    function onNodeSelect(fullName) {
        selectedId = fullName;
        showDetail(catalog.trouves[fullName]);
        highlightSidebarItem(fullName);
    }

    // --- Search ---
    function wireSearch() { /* attach input listener to #search */ }

    // --- Type filters ---
    function wireTypeFilters() { /* attach change listeners to #type-filters checkboxes */ }
})();
```

### 4.9 `graph.js` Module Design

`graph.js` exposes a global `ClairdocsGraph` object. It encapsulates all Cytoscape interactions.

```javascript
// graph.js -- Cytoscape.js graph rendering for clair docs

var ClairdocsGraph = (function() {
    "use strict";

    let cy = null;
    let onSelectCallback = null;

    function init(containerId, catalog, onSelect) {
        onSelectCallback = onSelect;

        // Register the dagre layout
        cytoscape.use(cytoscapeDagre);

        // Build elements from catalog
        const elements = buildElements(catalog);

        cy = cytoscape({
            container: document.getElementById(containerId),
            elements: elements,
            style: STYLE,       // the style array from section 4.3
            layout: LAYOUT,     // the layout config from section 4.3
            minZoom: 0.2,
            maxZoom: 3,
            wheelSensitivity: 0.3,
        });

        // Wire events
        cy.on("tap", "node", function(e) {
            selectNode(e.target.id());
        });
        cy.on("tap", function(e) {
            if (e.target === cy) { clearSelection(); }
        });
    }

    function buildElements(catalog) {
        const nodes = Object.values(catalog.trouves).map(t => ({
            group: "nodes",
            data: {
                id: t.full_name,
                label: t.table_name,
                fullName: t.full_name,
                type: t.type,
                databaseName: t.database_name,
                schemaName: t.schema_name,
                testCount: t.tests.length,  // computed client-side
            }
        }));

        const edges = catalog.edges.map(e => ({
            group: "edges",
            data: {
                id: e.source + "->" + e.target,
                source: e.source,
                target: e.target,
            }
        }));

        return nodes.concat(edges);
    }

    function selectNode(nodeId) {
        clearClasses();
        const node = cy.getElementById(nodeId);
        node.addClass("selected-node");

        // Compute full lineage
        const upstream = new Set();
        const downstream = new Set();
        walkPredecessors(node, upstream);
        walkSuccessors(node, downstream);

        const lineageNodes = new Set([nodeId, ...upstream, ...downstream]);

        cy.elements().forEach(ele => {
            if (ele.isNode()) {
                if (lineageNodes.has(ele.id())) {
                    if (ele.id() !== nodeId) ele.addClass("highlighted");
                } else {
                    ele.addClass("dimmed");
                }
            } else {
                // Edge: highlight if both endpoints are in lineage
                const sourceIn = lineageNodes.has(ele.source().id());
                const targetIn = lineageNodes.has(ele.target().id());
                if (sourceIn && targetIn) {
                    ele.addClass("highlighted");
                } else {
                    ele.addClass("dimmed");
                }
            }
        });

        if (onSelectCallback) onSelectCallback(nodeId);
    }

    function clearSelection() {
        clearClasses();
        if (onSelectCallback) onSelectCallback(null);
    }

    function clearClasses() {
        cy.elements().removeClass("selected-node highlighted dimmed");
    }

    function walkPredecessors(node, visited) {
        node.incomers("node").forEach(n => {
            if (!visited.has(n.id())) {
                visited.add(n.id());
                walkPredecessors(n, visited);
            }
        });
    }

    function walkSuccessors(node, visited) {
        node.outgoers("node").forEach(n => {
            if (!visited.has(n.id())) {
                visited.add(n.id());
                walkSuccessors(n, visited);
            }
        });
    }

    function panToNode(nodeId) {
        const node = cy.getElementById(nodeId);
        if (node.length) {
            cy.animate({ center: { eles: node }, duration: 300 });
        }
    }

    function setTypeFilter(activeTypes) {
        cy.nodes().forEach(node => {
            node.style("display", activeTypes.has(node.data("type")) ? "element" : "none");
        });
    }

    // Public API
    return {
        init: init,
        selectNode: selectNode,
        clearSelection: clearSelection,
        panToNode: panToNode,
        setTypeFilter: setTypeFilter,
    };
})();
```

---

## 5. pyproject.toml Changes

Hatchling includes all Python files by default, but it excludes non-Python files (like `.js`, `.html`) unless explicitly configured. Add a `[tool.hatch.build.targets.wheel]` section.

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/clair"]

[tool.hatch.build.targets.wheel.force-include]
"src/clair/docs/static" = "clair/docs/static"
```

This ensures all files under `src/clair/docs/static/` (HTML, JS) are included in the wheel at the correct path `clair/docs/static/`.

**Alternative (simpler):** Hatchling actually includes non-Python files inside packages by default when using the `packages` directive. But it's safer to be explicit with `force-include` since the static files are the load-bearing contract for this feature. If this doesn't work during implementation, fallback to adding a `[tool.hatch.build]` with `include` patterns.

---

## 6. Test Strategy

### 6.1 Unit Tests: Catalog Generation (`tests/unit/test_catalog.py`)

This is the most important test surface. The catalog is the contract between backend and frontend.

**What to test:**
1. `build_catalog` returns a dict with the correct top-level keys (`project_name`, `generated_at`, `clair_version`, `trouves`, `edges`).
2. Each entry in `trouves` matches `dag.get_trouve(fn).model_dump(mode="json")` -- i.e. `build_catalog` does not transform or lose any Trouve fields.
3. Edges list matches the DAG edges, with `source`/`target` keys.
4. `project_name` equals `project_root.name`.
5. `clair_version` equals `clair.__version__`.
6. `generated_at` is a valid ISO 8601 string.
7. Empty project (no Trouves) produces a dict with empty `trouves` and empty `edges`.
8. The returned dict is JSON-serializable (`json.dumps(catalog)` does not raise).

**How to test:** Use the existing `simple_project` fixture (it has a SOURCE + TABLE with dependencies). Build the DAG from it, call `build_catalog`, and assert on the returned dict's keys and values. These are pure function tests -- no I/O, no mocking needed.

### 6.2 Unit Tests: HTTP Server (`tests/unit/test_docs_server.py`)

**What to test:**
1. `GET /api/catalog.json` returns 200 with `application/json` content type and the catalog bytes.
2. `GET /` returns 200 with the index.html content.
3. `GET /app.js` returns 200 with the correct JS content.
4. `GET /nonexistent.txt` returns the index.html (SPA fallback).
5. `GET /../../etc/passwd` does not escape STATIC_DIR (returns 404 or index.html, not the file).

**How to test:** Instantiate `CatalogServer` on a random port (use port 0 for OS-assigned), start it in a background thread, make requests with `urllib.request`, assert on responses. Tear down in `finally` / fixture cleanup.

```python
import threading
from urllib.request import urlopen
from clair.docs.server import CatalogServer, CatalogHandler

def test_catalog_endpoint():
    server = CatalogServer(("127.0.0.1", 0), CatalogHandler)
    server.catalog_json = b'{"test": true}'
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    try:
        port = server.server_address[1]
        resp = urlopen(f"http://127.0.0.1:{port}/api/catalog.json")
        assert resp.status == 200
        assert resp.read() == b'{"test": true}'
    finally:
        server.shutdown()
```

### 6.3 What NOT to Unit Test

- **Frontend rendering**: The HTML/JS is not testable by Python tests. Manual testing in the browser is sufficient for MVP. Post-MVP, consider Playwright or Cypress for E2E.
- **`webbrowser.open`**: Side effect, not worth mocking.
- **Cytoscape layout quality**: Visual, not automatable.

### 6.4 Manual Test Checklist

The software-tester should verify these scenarios against the `example_2` project (50-Trouve project):

1. `clair docs --project example_2` opens the browser.
2. All 50 nodes visible in the graph, laid out left-to-right.
3. SOURCE nodes appear on the left, derived on the right.
4. Node colors match type (gray=SOURCE, blue=TABLE, light blue dashed=VIEW).
5. Clicking a node highlights upstream/downstream, dims others.
6. Detail panel shows all sections with correct data.
7. Sidebar search filters correctly on name, docs, and column names.
8. Type filter checkboxes hide/show nodes.
9. Graph is pannable and zoomable.
10. `--no-browser` flag prevents browser opening.
11. `--port 9999` binds to the specified port.
12. Using a port already in use shows a clear error.
13. Ctrl+C stops the server cleanly.

---

## 7. Integration with Existing Patterns

### 7.1 CLI Pattern

The `docs` command follows the same structure as `compile` and `dag`:
- Same `--project` option with same defaults and type.
- Same error handling: `ClairError` caught, logged via structlog, `sys.exit(1)`.
- Same discovery call: `discover_project(project_root)` then `build_dag(discovered)`.
- No `--profile` option (no Snowflake connection needed).
- No `--select` option (full project always, filtering is in the browser).

### 7.2 Logging

All log events use the `docs.*` namespace:
- `docs.start` -- logged after discovery, before serving.
- `docs.serving` -- logged when the server starts, includes the URL.
- `docs.stopped` -- logged on shutdown.
- `docs.error` / `docs.port_in_use` -- error cases.

This follows the established pattern: `compile.start`, `run.start`, `test.start`, etc.

### 7.3 Error Handling

- Discovery errors (bad Python files, cyclic imports): handled by the existing `ClairError` hierarchy, same as `compile`.
- Port in use: `OSError` caught specifically, clear message.
- No new exception classes needed.

### 7.4 Package Structure

`src/clair/docs/` is a new top-level package inside `clair`, alongside `adapters/`, `auth/`, `cli/`, `core/`, `trouves/`. This is the right level because:
- It's not a CLI command (so not in `cli/`).
- It's not core pipeline logic (so not in `core/`).
- It's a self-contained feature with its own static assets, its own server, and its own serialization logic.

The dependency graph is clean:

```
cli/main.py --imports--> docs/catalog.py --imports--> core/dag.py, clair.__version__
                         docs/server.py  --imports--> (stdlib only + structlog)
```

`docs/` depends on `core/` and `trouves/` (for the catalog), but nothing in `core/` or `trouves/` depends on `docs/`. The dependency is one-directional.

---

## 8. Implementation Order

Recommended sequence for the engineer:

1. **`src/clair/docs/__init__.py`** -- empty package marker with exports.
2. **`src/clair/docs/catalog.py`** -- implement `build_catalog`. This is trivial (see section 2.2) but is the core contract.
3. **`tests/unit/test_catalog.py`** -- write tests against the `simple_project` fixture. Assert on dict shape and that Trouve payloads match `model_dump`.
4. **`src/clair/docs/server.py`** -- implement `CatalogHandler` and `serve`.
5. **`tests/unit/test_docs_server.py`** -- write server route tests.
6. **`src/clair/docs/static/index.html`** -- HTML shell with all CSS and CDN `<script>` tags.
7. **`src/clair/docs/static/graph.js`** -- Cytoscape initialization and interaction.
8. **`src/clair/docs/static/app.js`** -- Sidebar, detail panel, search, filters.
9. **`src/clair/cli/main.py`** -- add the `docs` command.
10. **`pyproject.toml`** -- add hatch build config for static assets.
11. **Manual testing** against a real project.

Steps 2-3 can be done and validated in isolation before touching the frontend.
