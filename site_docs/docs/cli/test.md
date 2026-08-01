# clair test

Run data quality tests against live Snowflake tables.

```bash
clair test [--project PATH] [--env NAME] [--select PATTERN]... [--sample]
```

## Example

```bash
# Run all tests
clair test --project . --env dev

# Run tests for a specific schema
clair test --project . --env dev --select='refined.orders.*'

# Run with sampling (faster; skips TestRowCount)
clair test --project . --env dev --sample
```

## Behavior

- Requires a Snowflake connection (unlike `clair compile` and `clair dag`)
- Runs all tests attached to each selected Trouve
- SOURCE Trouves are skipped (they don't have tests)
- If no tests are attached to any selected Trouve, exits cleanly with a log message

## `--sample` mode

When `--sample` is set, most tests run against `SELECT TOP 1000 *` instead of the full table. `TestRowCount` is skipped because row counts are meaningless on a sample.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | `CLAIR_ENV` or `dev` | Environment name from `~/.clair/environments.yml` |
| `--select` | all | Glob pattern to filter Trouves. Repeat to union patterns. |
| `--sample` | `false` | Run tests against `SELECT TOP 1000 *` |

## Exit codes

- `0` — all tests passed (or no tests found)
- `1` — one or more tests failed or errored

## See also

- [Data Quality Tests](../guides/data-quality-tests.md)
- [Tests API reference](../reference/tests-api.md)
