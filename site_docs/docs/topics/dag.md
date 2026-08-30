# DAG

clair shows your project as a directed acyclic graph (DAG). Each node is a Trouve. Each edge is a dependency that clair reads from the Python imports.

## How clair builds the DAG

Each clair command starts with discovery:

1. Walks the full project directory and finds all the `.py` files that do not start with `_`
2. Imports each file as a Python module
3. Reads the imports. In `from some.path import trouve as alias`, if `some.path` is another Trouve file, clair adds a directed edge to the DAG.
4. Tests the graph for cycles. A circular dependency causes an error.

clair does not need a separate config file. It builds the full DAG from your Python import graph.

## Run order

Trouves run in topological order. Each dependency runs before its dependents.

If a node fails, clair skips all of its downstream dependents:

```
source.orders.raw      ✓  (SOURCE — no SQL, passthrough)
  └── refined.orders.daily   ✗  FAILED
        └── derived.orders.summary   —  SKIPPED (upstream failed)
```

## Read the DAG

Use `clair dag` to print the dependency tree:

```
$ cd ./my_project && clair dag

example_1_database.source.events (SOURCE)
└── example_1_database.refined.events (TABLE)
    ├── example_1_database.derived.daily_event_counts (TABLE)
    └── example_1_database.derived.top_event_types (TABLE)
```

Use `--select` to see a subgraph:

```bash
clair dag --select='mydb.refined.*'
```

## Artifacts

After `clair compile` or `clair run`, clair writes the compiled SQL to `_clairtifacts/<run_id>/`. The artifact tree has the same structure as your project:

```
_clairtifacts/
└── 019607ab3e8a7f1c8b2d4e6f0a1b2c3d/   ← UUIDv7 run_id
    ├── source/
    │   └── orders/
    │       └── raw.sql
    └── refined/
        └── orders/
            └── daily.sql
```

Add `_clairtifacts/` to your `.gitignore`.

## Files that discovery skips

Discovery skips these files:

- Files and directories with a name that starts with `_`, such as `_clairtifacts/` and `__pycache__/`
- `.git/`, `.venv/`
- `__database_config__.py` and `__schema_config__.py` — these are configuration files, not Trouves (see [Per-Database & Schema Config](per-database-schema-config.md))
