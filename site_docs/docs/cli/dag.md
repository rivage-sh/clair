# clair dag

Print the project dependency graph as an indented tree. No Snowflake connection required.

```bash
clair dag [--project PATH] [--select PATTERN]...
```

## Example

```bash
clair dag --project ./example_projects/example_1
```

Output:

```
example_1_database.source.events (SOURCE)
└── example_1_database.refined.events (TABLE)
    ├── example_1_database.derived.daily_event_counts (TABLE)
    └── example_1_database.derived.top_event_types (TABLE)
```

## Filtering with `--select`

View a subgraph by selecting specific Trouves:

```bash
clair dag --project . --select='refined.*.*'
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--select` | all | Glob pattern to filter Trouves. Repeat to union patterns. |

## See also

- [DAG concepts](../concepts/dag.md)
- [Selectors](../guides/selectors.md)
- [clair docs](docs.md) — interactive visual DAG in the browser
