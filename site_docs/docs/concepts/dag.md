# DAG

clair represents your project as a directed acyclic graph (DAG) where each node is a Trouve and each edge is a dependency derived from Python imports.

## How the DAG is built

When you run any clair command, it performs discovery:

1. Walk the project directory recursively, finding all `.py` files that don't start with `_`
2. Import each file as a Python module
3. Inspect imports — any `from some.path import trouve as alias` where `some.path` resolves to another Trouve file becomes a directed edge in the DAG
4. Validate that the graph is acyclic (circular dependencies raise an error)

No separate config file is needed. The DAG is fully derived from your Python import graph.

## Execution order

Trouves execute in topological order: dependencies always run before their dependents.

If a node fails, all of its downstream dependents are automatically skipped:

```
source.orders.raw      ✓  (SOURCE — no SQL, passthrough)
  └── refined.orders.daily   ✗  FAILED
        └── derived.orders.summary   —  SKIPPED (upstream failed)
```

## Viewing the DAG

Use `clair dag` to print the dependency tree:

```
$ clair dag --project ./my_project

example_1_database.source.events (SOURCE)
└── example_1_database.refined.events (TABLE)
    ├── example_1_database.derived.daily_event_counts (TABLE)
    └── example_1_database.derived.top_event_types (TABLE)
```

Use `--select` to view a subgraph:

```bash
clair dag --project . --select='mydb.refined.*'
```

## Artifacts

After `clair compile` or `clair run`, compiled SQL is written to `_clairtifacts/<run_id>/`, mirroring the directory structure:

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

## Files excluded from discovery

The following are skipped during discovery:

- Files and directories starting with `_` (including `_clairtifacts/`, `__pycache__/`)
- `.git/`, `.venv/`
- `__database_config__.py` and `__schema_config__.py` — these are configuration files, not Trouves (see [Per-Database & Schema Config](../guides/per-database-schema-config.md))
