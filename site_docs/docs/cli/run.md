# clair run

Execute Trouves against Snowflake in topological dependency order, then run data quality tests.

```bash
clair run [--project PATH] [--env NAME] [--select PATTERN]... [--run-mode MODE] [--no-test] [--sample]
```

## Example

```bash
# Run all Trouves in the project
clair run --project . --env dev

# Run only the orders schema
clair run --project . --env dev --select='refined.orders.*'

# Force a full refresh (ignore incremental config)
clair run --project . --env prod --run-mode full_refresh

# Skip tests
clair run --project . --env dev --no-test
```

## Execution order

Trouves run in topological order — dependencies always execute before their dependents. If a node fails, all downstream dependents are skipped automatically.

SOURCE Trouves pass through (no SQL is executed against them).

## Tests

After each successful TABLE or VIEW, attached tests run automatically. If any test fails, the run exits with a non-zero status code. Use `--no-test` to skip tests.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | `CLAIR_ENV` or `dev` | Environment name from `~/.clair/environments.yml` |
| `--select` | all | Glob pattern to filter Trouves. Repeat to union patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Overrides each Trouve's `run_config`. |
| `--no-test` | `false` | Skip data quality tests |
| `--sample` | `false` | Run tests against `SELECT TOP 1000 *` (skips `TestRowCount`) |

## Exit codes

- `0` — all Trouves succeeded and all tests passed
- `1` — one or more Trouves failed, or one or more tests failed

## See also

- [Selectors](../guides/selectors.md)
- [Incrementality](../guides/incrementality.md)
- [Data Quality Tests](../guides/data-quality-tests.md)
