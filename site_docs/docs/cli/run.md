# clair run

Run Trouves against Snowflake in topological order, then run the data quality tests.

```bash
clair run [--project PATH] [--env NAME] [--select PATTERN]... [--run-mode MODE] [--no-test] [--sample]
```

## Example

```bash
# Run all Trouves in the project
clair run --project . --env dev

# Run only the orders schema
clair run --project . --env dev --select='refined.orders.*'

# Force a full refresh (ignore the incremental config)
clair run --project . --env prod --run-mode full_refresh

# Skip tests
clair run --project . --env dev --no-test
```

## Run order

Trouves run in topological order — each dependency runs before its dependents. If a node fails, clair skips all the downstream dependents.

clair does not run SQL against a SOURCE Trouve. The routing entry still gives the
address that clair reads.

## Tests

clair runs the attached tests after each successful TABLE or VIEW. If a test fails, the run stops with a non-zero status code. Use `--no-test` to skip the tests.

The tests decide the publication. They do not report a fault after it reaches production: clair writes each Trouve to a run-scoped staging address, runs the tests there, and gives the data its physical address only after each test passes. See [Staging](../guides/staging.md).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `.` | Path to the clair project root |
| `--env` | `CLAIR_ENV` or `dev` | Environment name from `~/.clair/environments.yml` |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../guides/selectors.md). Repeat the flag to add more patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Overrides the `run_config` of each Trouve. |
| `--no-test` | `false` | Skip the data quality tests |
| `--sample` | `false` | Run the tests against `SELECT TOP 1000 *` (skips `TestRowCount`) |

## Exit codes

- `0` — all the Trouves ran, and all the tests passed
- `1` — one or more Trouves failed, or one or more tests failed. A Trouve whose
  tests failed counts as a failure. Its physical object keeps the data that it
  had, and the rejected candidate stays for you to query.

## See also

- [Selectors](../guides/selectors.md)
- [Incrementality](../guides/incrementality.md)
- [Data Quality Tests](../guides/data-quality-tests.md)
- [Staging](../guides/staging.md)
