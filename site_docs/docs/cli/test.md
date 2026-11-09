# clair test

Run data quality tests against live Snowflake tables.

```bash
clair test --env NAME [--select PATTERN]... [--sample [--sample-rows N]] [--threads N]
```

## Example

```bash
# Run all tests
clair test --env dev

# Run tests for a specific schema
clair test --env dev --select='refined.orders.*'

# Run against a sample (faster; skips TestRowCount)
clair test --env dev --sample

# Test 8 Trouves at one time
clair test --env dev --threads 8
```

## Behavior

- The command needs a Snowflake connection. `clair compile` and `clair dag` do not.
- The command runs all the tests that you attach to each selected Trouve.
- clair skips SOURCE Trouves. They do not have tests.
- If no selected Trouve has tests, the command writes a log message and stops with success.
- clair tests more than one Trouve at one time. The tests of one Trouve always run
  one after the other. Each thread holds a private Snowflake connection, and the
  output keeps the same order at each thread count. See
  [Environments](../topics/environments.md).

## `--sample` mode

If you give `--sample`, each test reads the sample of its Trouve instead of the full table: the `sample_sql` of the Trouve, or `--sample-rows` rows. clair skips `TestRowCount` and `TestSql`, because a row count has no meaning on a sample. A test that gives `run_with_sample` replaces that default. See [Sampled Runs](../topics/sampling.md).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--env` | required | The environment name. It names a key in `~/.clair/environments.yml` and an entry in `__routing__.py`. `CLAIR_ENV` gives it when you omit the flag. |
| `--select` | all | Pattern that filters the Trouves. It accepts a glob and the `+` graph operator. See [Selectors](../topics/selectors.md). Repeat the flag to add more patterns. |
| `--sample` | `false` | Run each test against the sample of its Trouve |
| `--sample-rows` | `1000` | The rows of each sample, for a Trouve without a `sample_sql`. It needs `--sample`. |
| `--threads` | the `threads` field of the environment, or `4` | The number of Trouves that clair tests at one time. Each thread holds one Snowflake connection. |

## Exit codes

- `0` — all the tests passed, or the project has no tests
- `1` — one or more tests failed, or gave an error

## See also

- [Data Quality Tests](../topics/data-quality-tests.md)
- [Tests API reference](../reference/tests-api.md)
