# clair compile

Resolve the DAG and write the generated SQL to `_clairtifacts/`. The command does not connect to Snowflake.

```bash
clair compile [--project PATH] [--env NAME] [--select PATTERN]... [--run-mode MODE]
```

## Example

```bash
# Compile the whole project
clair compile --project .

# Compile and apply routing (requires --env)
clair compile --project . --env prod

# Compile only the orders schema
clair compile --project . --select='refined.orders.*'
```

## What it does

1. Discovers all Trouves in the project
2. Resolves import references and builds the DAG
3. Replaces the f-string placeholders with real Snowflake names. It applies routing if you give `--env`.
4. Writes SQL files to `_clairtifacts/<run_id>/`

## Artifact layout

```
_clairtifacts/
└── 019607ab3e8a7f1c8b2d4e6f0a1b2c3d/    ← UUIDv7 run_id
    ├── source/
    │   └── orders/
    │       └── raw.sql
    └── refined/
        └── orders/
            └── daily.sql
```

## When to use compile

- **Review the SQL before the run** — read what clair will run
- **CI compilation step** — find import errors and broken references without a Snowflake connection
- **Audit trail** — commit the artifacts for a point-in-time record of the generated SQL

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | optional | Environment name. Necessary if clair must apply routing to the generated SQL. |
| `--select` | all | Glob pattern that filters the Trouves. Repeat the flag to add more patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Selects which SQL variant clair generates. |

## See also

- [DAG](../concepts/dag.md)
- [Selectors](../guides/selectors.md)
- [clair clean](clean.md)
