# clair compile

Resolve the DAG and write the generated SQL to `_clairtifacts/`. The command does not connect to Snowflake.

```bash
clair compile [--project PATH] [--env NAME] [--select PATTERN]... [--run-mode MODE]
```

## Example

```bash
# Compile the whole project
clair compile

# Compile and apply routing (requires --env)
clair compile --env prod

# Compile only the orders schema
clair compile --select='refined.orders.*'
```

Each plan shows the staged path: the build at the staging address, a comment that marks the test step, and the promotion. See [Staging](../topics/staging.md).

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
| `--project` | the root search | Path to the clair project root. Without it, clair walks up from the working directory to the first `__clair_project__.py` |
| `--env` | optional | Environment name. It selects the entry in `__routing__.py` that clair applies to the generated SQL. |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Selects which SQL variant clair generates. |

## See also

- [DAG](../topics/dag.md)
- [Selectors](../topics/selectors.md)
- [Staging](../topics/staging.md)
- [clair clean](clean.md)
