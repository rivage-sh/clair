# Sampled Runs

A development build over large inputs is slow and expensive. `clair run --sample` builds
each Trouve from a time window of its inputs, and not from the complete inputs:

```bash
clair run --env dev --sample '3 days'
clair run --env dev --sample 2024-07-01/2024-07-08
```

The flag gives one window to the complete run. Each reference to a Trouve that gives an
`event_time` reads only the rows of that window. A reference to a Trouve without an
`event_time` reads each row. The flags of the command are in [clair run](../cli/run.md#sampled-run).

## Why a time window, and not a random sample

A random sample, for example `SELECT TOP 1000`, takes each input alone. Two inputs that you
join then keep different keys: the 1000 orders and the 1000 order lines have almost no
order in common, and the join gives almost no rows. The result looks like a fault in your
SQL.

A time window keeps the joins whole. An order and its lines happen at the same time, thus
they fall in the same window. dbt makes the same choice for its `--sample` flag.

## Give each Trouve its event time

`event_time` names the column that holds the time of each row. Give it on each Trouve that
a sample can filter: a `Source`, a `Table`, a `View`, a `PandasTable` or a `SeedTable`.

```python
# example_1_database/source/events.py
from clair import Column, ColumnType, Source

trouve = Source(
    docs="An events table that exists before clair runs. It holds the raw user interactions.",
    # `clair run --sample '3 days'` reads only the events of the last 3 days.
    event_time="occurred_at",
    columns=[
        Column(name="event_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="occurred_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="properties", type=ColumnType.VARIANT),
    ],
)
```

A Trouve that reads `{example_1_database_source_events}` then reads this SQL in a sampled
run:

```sql
(SELECT * FROM example_1_database.source.events
 WHERE occurred_at >= '2024-07-01 00:00:00.000000'::TIMESTAMP_NTZ
   AND occurred_at <  '2024-07-08 00:00:00.000000'::TIMESTAMP_NTZ)
```

The window holds its start, and not its end. clair gives each boundary in UTC.

Give an `event_time` to a Trouve of the run too. A sampled build of
`example_1_database.refined.events` holds three days of events, and a reader of it gets the
same filter.

## A dimension table needs each row

A filter on the wrong table is as bad as a random sample. The customers who signed up in
the last three days are not the customers of the orders of the last three days. If a
customers table gives an `event_time`, read it with `unsampled()`:

```python
trouve = Table(
    sql=f"""
        SELECT o.order_id, c.customer_name
        FROM {orders} AS o
        JOIN {customers.unsampled()} AS c ON o.customer_id = c.customer_id
    """,
)
```

`unsampled()` makes the same DAG edge as `{customers}`. Only the sampled run sees a
difference.

A sampled reference becomes a subquery, and a subquery cannot qualify a column. clair
stops the run if the SQL writes `{orders}.order_id`. Give the reference an alias, as above.

## A sample never reaches production

A sampled build holds a part of the data. If it replaced a production table, each
dashboard would show three days of history, and nothing would tell the reader.

clair stops a sampled run before it connects if a selected Trouve writes to its logical
address. The logical address is the production address, and the routing entry of the
environment decides the physical address. Thus the rule follows the environment: the `dev`
entry of `example_1` sends each Trouve to the database of the developer, and `--sample`
runs there. The `prod` entry gives each address back, and `--sample` stops:

```
A sampled run must not write to a production address, but the environment 'prod'
routes these Trouves to their logical address: example_1_database.derived.daily_event_counts, ...
```

A SOURCE writes nothing, thus it never stops the run. See [Routing](routing.md).

## The tests of a sampled run

The tests read the complete object that the run built, because that object is the sample.
clair skips each test that needs the complete data: `TestRowCount`, because a row count
has no meaning on a window, and `TestSql`, because your SQL can read a second Trouve that
the run did not sample. A test that passes then decides the promotion, as in each run. See
[Staging](staging.md).

`clair test --sample` is a different thing. It runs the tests of the existing objects
against `SELECT TOP 1000 *`, and it builds nothing. See [clair test](../cli/test.md).

## Limits

- **A seed is complete.** A `SeedTable` reads no input, thus a sample builds each of its
  rows. Its `event_time` filters the readers of the seed.
- **The SQL of a test reads each row.** A test examines the object that the run built, and
  a test that reads a second Trouve needs that Trouve complete.
- **A TIMESTAMP_TZ or a TIMESTAMP_LTZ column.** clair compares the column with two
  TIMESTAMP_NTZ values in UTC. Snowflake converts such a column with the time zone of the
  session, thus a boundary can move by the offset of that time zone. A TIMESTAMP_NTZ
  column that holds UTC, and a DATE column, compare exactly.
- **An address that you type as text.** clair samples a reference token only. A table name
  in the SQL text stays as you wrote it, as in each run.
