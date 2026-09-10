# clair run

Run Trouves against Snowflake in dependency order, then run the data quality tests.

```bash
clair run [--env NAME] [--select PATTERN]... [--run-mode MODE] [--no-test] [--sample] [--threads N]
```

## Example

```bash
# Run all Trouves in the project
clair run --env dev

# Run only the orders schema
clair run --env dev --select='refined.orders.*'

# Force a full refresh (ignore the incremental config)
clair run --env prod --run-mode full_refresh

# Skip tests
clair run --env dev --no-test

# Run 8 Trouves at one time
clair run --env dev --threads 8
```

## Run order

clair starts a Trouve when each Trouve that it imports completed. If a node fails, clair skips all the downstream dependents. clair continues with the other branches.

clair does not run SQL against a SOURCE Trouve. The routing entry still gives the
address that clair reads.

## Parallel execution

clair runs more than one Trouve at one time. `--threads` gives the count, and the
`threads` field of the environment gives the default. See
[Environments](../topics/environments.md).

Each thread holds a private Snowflake connection, and clair opens each connection
before the first Trouve starts. A connection holds the role and the warehouse of
the session, so two Trouves that need a different `warehouse` cannot share one.

The output comes in completion order, not in DAG order. A quick Trouve that
started second can thus report before a slow Trouve that started first.

Three limits to know:

- **A thread makes no compute.** Each thread sends its queries to the one
  warehouse of the environment. A warehouse runs `MAX_CONCURRENCY_LEVEL`
  statements at one time. The default is 8. It queues the statements above that
  number. A multi-cluster warehouse in the auto-scale mode starts another
  cluster instead, and Snowflake bills each cluster that runs. Thus a thread
  count above 8 gives no more parallel work on a warehouse that keeps the
  default, and it can raise your bill on a multi-cluster warehouse.
- **A connection costs no credits.** Snowflake bills the warehouse per second
  while it runs, with a minimum of 60 seconds each time it starts. A session
  that sends no query starts no warehouse. A connection costs one login, thus a
  high thread count makes the start slower, and nothing more. clair opens no
  more connections than the run has Trouves.
- **SSO opens one browser window.** With `authenticator: externalbrowser`, clair
  asks the connector to keep the login token. The second connection reads that
  token, thus a parallel run does not open one window for each thread.

## Tests

clair runs the attached tests after each successful TABLE or VIEW. If a test fails, the run stops with a non-zero status code. Use `--no-test` to skip the tests.

The tests decide the publication. They do not report a fault after it reaches production: clair writes each Trouve to a run-scoped staging address, runs the tests there, and gives the data its physical address only after each test passes. See [Staging](../topics/staging.md).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--env` | `CLAIR_ENV` or `dev` | Environment name from `~/.clair/environments.yml` |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |
| `--run-mode` | `full_refresh` | `full_refresh` or `incremental`. Overrides the `run_config` of each Trouve. |
| `--no-test` | `false` | Skip the data quality tests |
| `--sample` | `false` | Run the tests against `SELECT TOP 1000 *` (skips `TestRowCount`) |
| `--threads` | the `threads` field of the environment, or `4` | The number of Trouves that run at one time. Each thread holds one Snowflake connection. |

## Exit codes

- `0` — all the Trouves ran, and all the tests passed
- `1` — one or more Trouves failed, or one or more tests failed. A Trouve whose
  tests failed counts as a failure. Its physical object keeps the data that it
  had, and the rejected candidate stays for you to query.

## See also

- [Selectors](../topics/selectors.md)
- [Incrementality](../topics/incrementality.md)
- [Data Quality Tests](../topics/data-quality-tests.md)
- [Staging](../topics/staging.md)
