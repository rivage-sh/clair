# clair compile

Resolve the DAG and write generated SQL to `_clairtifacts/`. No Snowflake connection is made.

```bash
clair compile [--project PATH] [--env NAME] [--select PATTERN]... [--run-mode MODE]
```

## Example

```bash
# Compile the whole project
clair compile --project .

# Compile with routing applied (requires --env)
clair compile --project . --env prod

# Compile only the orders schema
clair compile --project . --select='refined.orders.*'
```

## What it does

1. Discovers all Trouves in the project
2. Resolves import references and builds the DAG
3. Substitutes f-string placeholders with real Snowflake names (applying routing if `--env` is provided)
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

- **Review SQL before running** — inspect what clair will execute
- **CI compilation check** — catch import errors and broken references without a Snowflake connection
- **Audit trail** — commit artifacts for a point-in-time record of generated SQL

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | optional | Environment name. Required if you want routing applied to generated SQL. |
| `--select` | all | Glob pattern to filter Trouves. Repeat to union patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Affects which SQL variant is generated. |

## See also

- [DAG](../concepts/dag.md)
- [Selectors](../guides/selectors.md)
- [clair clean](clean.md)
