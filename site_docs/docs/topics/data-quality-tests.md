# Data Quality Tests

A test is a Python object in the `tests` list of a Trouve. Each test is a query that gives
the rows that break a rule. Zero rows is a pass. One row or more is a failure.

```python
from clair import THIS, Table, TestNotNull, TestRowCount, TestSql, TestUnique

trouve = Table(
    sql=f"SELECT * FROM {orders}",
    tests=[
        TestUnique(column="order_id"),
        TestNotNull(column="customer_id"),
        TestRowCount(min_rows=1),
        TestSql(sql=f"SELECT * FROM {THIS} WHERE amount < 0"),
    ],
)
```

The test sits beside the SQL that it examines. Thus a reviewer reads the rule and the
query in one file, and a change of the SQL and of its test goes in one commit.

The [Tests API reference](../reference/tests-api.md) gives each test type, its fields, and
the SQL that it makes. `TestSql` takes your own query, with `{THIS}` for the Trouve that
holds the test.

## A test decides the promotion

Snowflake can query a table only after the table exists. A test after a direct write thus
tells you that production is already wrong.

clair changes the order. `clair run` builds each Trouve at a
[staging address](staging.md), runs its tests against that object, and promotes it only
when no blocking test fails. A failure keeps the production object as it was, keeps the
rejected candidate for you to query, and skips each Trouve downstream. The other branches
of the DAG continue.

`clair run --no-test` writes production directly, and runs no test.

## Severity

Some conditions deserve a message, and not a stop: a few late rows, or a count that
drifts. Give such a test `severity=TestSeverity.WARN`:

```python
from clair import Table, TestRowCount, TestSeverity, TestUnique

trouve = Table(
    sql=f"SELECT * FROM {orders}",
    tests=[
        TestUnique(column="order_id"),
        TestRowCount(min_rows=1000, severity=TestSeverity.WARN),
    ],
)
```

| | The test finds rows | The query of the test fails |
|-|---------------------|-----------------------------|
| `ERROR`, the default | Stops the promotion. Status code 1. | Stops the promotion. Status code 1. |
| `WARN` | Shows `WARN` with the row count. clair promotes the Trouve. Status code 0. | Stops the promotion. Status code 1. |

A `WARN` test whose query fails still stops the promotion. Then clair knows nothing about
the data, and the fault is in the test, not in the data.

Each `TestResult` holds `severity` and `is_blocking`, thus an orchestrator can send an
alert for each warning. See [`TestResult`](../reference/python-api.md#testresult).

## Run the tests alone

`clair test` runs the tests against the objects that are in Snowflake now. It builds
nothing, and promotes nothing. Each Trouve runs its tests with its own `role`,
`warehouse`, `query_tags` and timeout.

```bash
clair test --env dev
clair test --env dev --select='refined.orders.*'
```

## Tests on a sample

`clair test --sample` runs each test against the sample of its Trouve. `clair run --sample`
builds each Trouve from a sample of its inputs, and the tests read the complete object that
the run built.

A rule that the complete data obeys can fail on a sample. Thus both commands skip
`TestRowCount` and `TestSql` by default. `run_with_sample=True` or `False` on a test
replaces that default. See [Sampled Runs](sampling.md).
