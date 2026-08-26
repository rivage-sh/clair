# clair dag

Print the dependency graph of the project as an indented tree. The command does not need a Snowflake connection.

```bash
clair dag [--project PATH] [--select PATTERN]...
```

## Example

```bash
clair dag --project ./examples/projects/example_1
```

Output:

```
example_1_database.source.events (SOURCE)
└── example_1_database.refined.events (TABLE)
    ├── example_1_database.derived.daily_event_counts (TABLE)
    └── example_1_database.derived.top_event_types (TABLE)
```

## Filter with `--select`

Select specific Trouves to see a subgraph:

```bash
clair dag --project . --select='refined.*.*'
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |

## See also

- [DAG](../topics/dag.md)
- [Selectors](../topics/selectors.md)
- [clair docs](docs.md) — interactive visual DAG in the browser
