# clair docs

Start a local web UI showing the project DAG and per-Trouve documentation. No Snowflake connection required.

```bash
clair docs [--project PATH] [--port PORT] [--host HOST] [--no-browser]
```

## Example

```bash
# Open in browser (default)
clair docs --project .

# Custom port
clair docs --project . --port 9000

# Don't open browser automatically
clair docs --project . --no-browser
```

## What the UI shows

- **Interactive DAG** — pan, zoom, and click nodes to explore the graph
- **Sidebar** — searchable list of all Trouves, filterable by type (SOURCE / TABLE / VIEW)
- **Detail panel** — for each selected Trouve:
    - Full name and type
    - Documentation string
    - Column definitions
    - SQL query
    - Attached tests
    - Run configuration
    - Upstream and downstream lineage
    - File path

## Stopping the server

Press `Ctrl+C` to stop the server.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--port` | `8741` | Port for the local docs server |
| `--host` | `127.0.0.1` | Bind address |
| `--no-browser` | `false` | Do not open the browser automatically |

If the port is already in use, clair exits with an error suggesting `--port <other>`.

## See also

- [clair dag](dag.md) — text-based DAG output
