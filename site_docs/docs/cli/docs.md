# clair docs

Start a local web UI for the project DAG and the documentation of each Trouve. The command does not need a Snowflake connection.

```bash
clair docs [--project PATH] [--port PORT] [--host HOST] [--no-browser]
```

## Example

```bash
# Open in browser (default)
clair docs

# Custom port
clair docs --port 9000

# Do not open the browser automatically
clair docs --no-browser
```

## What the UI shows

- **Interactive DAG** — pan, zoom, and click the nodes to explore the graph
- **Sidebar** — a list of all the Trouves. You can search it, and filter it by type (SOURCE / TABLE / VIEW).
- **Detail panel** — for each selected Trouve:
    - Full name and type
    - Documentation string
    - Column definitions
    - SQL query
    - Attached tests
    - Run configuration
    - Upstream and downstream lineage
    - File path

## Stop the server

Press `Ctrl+C` to stop the server.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | the root search | Path to the clair project root. Without it, clair walks up from the working directory to the first `__clair_project__.py` |
| `--port` | `8741` | Port for the local docs server |
| `--host` | `127.0.0.1` | Bind address |
| `--no-browser` | `false` | Do not open the browser automatically |

If a different program uses the port, clair stops with an error. The error tells you to use `--port <other>`.

## See also

- [clair dag](dag.md) — text-based DAG output
