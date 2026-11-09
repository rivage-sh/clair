# clair compile

Resolve the DAG and write the generated SQL to `_clairtifacts/`. The command does not connect to Snowflake.

```bash
clair compile --env NAME [--select PATTERN]... [--run-mode MODE] [--sample [--sample-rows N]]
```

## Example

```bash
# Compile the whole project with the dev routing
clair compile --env dev

# Compile with the prod routing
clair compile --env prod

# Compile only the orders schema
clair compile --env dev --select='refined.orders.*'

# Show the SQL of a sampled run
clair compile --env dev --sample --sample-rows 100
```

Each plan shows the staged path: the build at the staging address, a comment that marks the test step, and the promotion. See [Staging](../topics/staging.md).

## What it does

1. Discovers all Trouves in the project
2. Resolves import references and builds the DAG
3. Replaces the f-string placeholders with real Snowflake names. It applies the routing entry of `--env`.
4. Writes one file for each Trouve to `_clairtifacts/<run_id>/`: a `.sql` file for a SQL Trouve, and a `.py` file for a pandas Trouve

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
| `--env` | required | The environment name. It names a key in `~/.clair/environments.yml` and an entry in `__routing__.py`. `CLAIR_ENV` gives it when you omit the flag. |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Selects which SQL variant clair generates. |
| `--sample` | off | Show the SQL of `clair run --sample`. Each Trouve that the run does not build reads its sample. A selected Trouve must write to an address that routing changed. See [Sampled Runs](../topics/sampling.md). |
| `--sample-rows` | `1000` | The rows of each sample, for a Trouve without a `sample_sql`. It needs `--sample`. |

## See also

- [DAG](../topics/dag.md)
- [Selectors](../topics/selectors.md)
- [Staging](../topics/staging.md)
- [clair clean](clean.md)
