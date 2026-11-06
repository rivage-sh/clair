# Sampled Runs

A development build over large inputs is slow and expensive. Most of the time you want to
know a smaller thing: does each Trouve compile, does each column exist, and does the DAG
run from one end to the other? `clair run --sample` answers that question with a part of
the data:

```bash
clair run --env dev --sample
clair run --env dev --sample --sample-rows 1000
```

The flags of the command are in [clair run](../cli/run.md#sampled-run).

## What a sample replaces

A sampled run reads a sample of each Trouve that it does not build: each `Source`, and each
Trouve outside the selection. A reference to such a Trouve becomes a subquery:

```sql
-- FROM {orders} AS o, in a sampled run
FROM (SELECT * FROM raw.shop.orders LIMIT 1000) AS o
```

A Trouve that the run builds is a sample already, because it read sampled inputs. Thus a
reference to it keeps its address. A second sample at each layer would make each table of
the DAG smaller than the table before it.

A pandas input reads the same subquery, thus the two backends read the same rows.

## The row limit, and its limit

A Trouve without a `sample_sql` gives `--sample-rows` rows. The default is 1000.

A `LIMIT` without an `ORDER BY` gives no specific rows. Snowflake can stop the scan early,
thus the sample is cheap, but it is not random. Two inputs that you join keep
different keys: 1000 orders and 1000 order lines can have almost no order in common, and
the join gives almost no rows.

That is correct for the question above. The SQL compiles, each column exists, and each
Trouve builds. It is not a test of the transformation. A table that the join empties gives
a column test that passes on no rows, and a pandas transform that runs on an empty
DataFrame.

## A sample that you write: `sample_sql`

When a row limit is not enough, give the Trouve a `sample_sql`. A sample then reads that
query in place of the Trouve. `{THIS}` is the address of the Trouve:

```python
# example_1_database/source/events.py
from clair import THIS, Column, ColumnType, Source

trouve = Source(
    docs="An events table that exists before clair runs. It holds the raw user interactions.",
    # `clair run --sample` reads the events of two users, and not a row limit. Each
    # Trouve downstream then holds the complete history of those two users.
    sample_sql=f"SELECT * FROM {THIS} WHERE user_id IN ('usr_abc', 'usr_def')",
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="properties", type=ColumnType.VARIANT),
    ],
)
```

A filter on a key keeps the joins whole. Give the same users to each input that holds a
user, and each join downstream gives the correct rows for those users. The result of each
Trouve is then correct for that set, and not only valid SQL.

### A sample that reads another Trouve

A `sample_sql` is an f-string, as `sql` is. It can read another Trouve, for example a seed
that holds the test users:

```python
from mydb.reference.test_users import trouve as test_users

from clair import THIS, Source

trouve = Source(
    sample_sql=f"SELECT * FROM {THIS} WHERE user_id IN (SELECT user_id FROM {test_users})",
)
```

The seed is then an upstream Trouve of the source, in each run and not only in a sampled
run. Thus `--sample` does not change the DAG, and `--select +<reader>` selects the seed.
A reference inside a `sample_sql` reads the address of its Trouve, and never a second
sample.

### A table that needs each row

A small dimension table can need each of its rows. Give it a `sample_sql` that reads all
of them:

```python
trouve = Source(sample_sql=f"SELECT * FROM {THIS}")
```

### The same rule for many Trouves

`sample_sql` is the field of one Trouve. `Trouve.sample(sample_rows)` gives the subquery
for each Trouve, thus a subclass that overrides it changes the sample of each Trouve of
that class. See the [Trouve API](../reference/trouve-api.md#methods).

A sampled reference becomes a subquery, and a subquery cannot qualify a column. clair
stops the run if the SQL writes `{orders}.order_id`. Give the reference an alias, as in
`FROM {orders} AS o`.

## A sample never reaches production

A sampled build holds a part of the data. If it replaced a production table, each
dashboard would show a part of its history, and nothing would tell the reader.

clair stops a sampled run before it connects if a selected Trouve writes to its logical
address. The logical address is the production address, and the routing entry of the
environment decides the physical address. Thus the rule follows the environment: the `dev`
entry of `example_1` sends each Trouve to the database of the developer, and `--sample`
runs there. The `prod` entry gives each address back, and `--sample` stops:

```
A sampled run must not write to a production address, but the environment 'prod'
routes these Trouves to their logical address: example_1_database.derived.daily_event_counts, ...
```

A `Source` writes nothing, thus it never stops the run. See [Routing](routing.md).

## The tests of a sampled run

The tests read the complete object that the run built, because that object is the sample.

A test that passes on a sample proves nothing about the complete data. A test that fails
on a sample usually shows a fault in the complete data too: a duplicate key in a part of the
rows is a duplicate key in all of them. But some tests fail on a sample when the complete
data obeys them:

- A row count fails, because a sample holds fewer rows.
- Your own `TestSql` can read a Trouve that the run did not sample.
- A `LEFT JOIN` to an input that the sample cut gives a NULL where the complete data holds
  a value. A `TestNotNull` on that column then fails.

clair skips `TestRowCount` and `TestSql` on a sample, and runs the other tests. Give
`run_with_sample=False` to a test of the third kind.

`run_with_sample` on one test replaces that default:

```python
TestUnique(column="order_id", run_with_sample=False)
TestRowCount(min_rows=1, run_with_sample=True)
```

A test that passes then decides the promotion, as in each run. See [Staging](staging.md).

## `clair test --sample`

`clair test --sample` builds nothing. Each test reads the sample of its own Trouve, from
the same `Trouve.sample()`, and skips the same tests. See [clair test](../cli/test.md).
