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

# Run against a sample (faster; skips TestRowCount)
clair test --project . --env dev --sample
```

## Behavior

- The command needs a Snowflake connection. `clair compile` and `clair dag` do not.
- The command runs all the tests that you attach to each selected Trouve.
- clair skips SOURCE Trouves. They do not have tests.
- If no selected Trouve has tests, the command writes a log message and stops with success.

## `--sample` mode

If you give `--sample`, most tests run against `SELECT TOP 1000 *` instead of the full table. clair skips `TestRowCount`, because a row count has no meaning on a sample.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | `CLAIR_ENV` or `dev` | Environment name from `~/.clair/environments.yml` |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |
| `--sample` | `false` | Run the tests against `SELECT TOP 1000 *` |

## Exit codes

- `0` — all the tests passed, or the project has no tests
- `1` — one or more tests failed, or gave an error

## See also

- [Data Quality Tests](../topics/data-quality-tests.md)
- [Tests API reference](../reference/tests-api.md)
