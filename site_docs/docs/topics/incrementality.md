# Incrementality

By default, clair makes every Trouve again on each run with `CREATE OR REPLACE TABLE`. For a large table, you can configure an incremental strategy. The strategy applies the new data only.

## Default: full refresh

```python
trouve = Table(
    sql=f"SELECT * FROM {upstream}",
    # No run_config — clair does a full refresh
)
```

Generated SQL:

```sql
CREATE OR REPLACE TABLE example_3_database.refined.orders AS (
    SELECT * FROM example_3_database.source.orders
)
```

## `Append`

`Append` adds the new rows only on each incremental run. Use it for event streams and for logs that you add to.

Read `clair.run_mode` to change the SQL between a full refresh and an incremental run:

```python
# example_3_database/derived/recent_orders.py
from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import Append, Column, ColumnType, Incremental, RunMode, Table

sql = f"""
    select
        order_id,
        customer_id,
        order_status,
        amount,
        created_at,
        created_date
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_at > dateadd('day', -3, current_timestamp())
    """

trouve = Table(
    docs="""
        Incremental append of the recent orders. In each incremental run, clair adds the
        orders from the last 3 days. Thus the table keeps a log of the recent activity.

        Note: the lookback of 3 days gives a small overlap. The overlap catches the rows that
        come late. In a full refresh, clair selects all rows and uses no date filter.
    """,
    sql=sql,
    run_config=Incremental(strategy=Append()),
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="created_date", type=ColumnType.DATE),
    ],
)
```

Generated SQL on an incremental run:

```sql
INSERT INTO example_3_database.derived.recent_orders
SELECT * FROM (
    select order_id, customer_id, order_status, amount, created_at, created_date
    from example_3_database.refined.orders
    where created_at > dateadd('day', -3, current_timestamp())
)
```

## `Upsert`

`Upsert` merges the new data into the target table. It updates the rows that exist and inserts the new rows. Use it for a table that changes slowly, such as an aggregate for each customer.

```python
# example_3_database/derived/customer_order_summary.py
from example_3_database.refined.orders import trouve as refined_orders

from clair import Column, ColumnType, Incremental, Table, TrouveType, Upsert

trouve = Table(
    docs="""
        Order statistics for each customer. clair merges the rows on customer_id.

        In each incremental run, clair calculates the statistics again from all the refined
        orders and merges them into the target table. clair updates the customers that exist
        and inserts the new customers. Thus the table always shows the current totals, and
        clair does not make the full table again.

        In a full refresh, clair makes the table again with CREATE OR REPLACE.
    """,
    sql=f"""
        select
            customer_id,
            count(*)                as total_orders,
            sum(amount)             as total_amount,
            min(created_at)         as first_order_at,
            max(created_at)         as last_order_at,
            max(updated_at)         as last_updated_at
        from {refined_orders}
        group by customer_id
    """,
    run_config=Incremental(strategy=Upsert(primary_key_columns=["customer_id"])),
    columns=[
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="total_amount", type=ColumnType.FLOAT),
        Column(name="first_order_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="last_order_at", type=ColumnType.TIMESTAMP_NTZ),
        Column(name="last_updated_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
```

!!! note
    `Upsert` needs `columns`. clair uses the column list to build the MERGE statement.

Generated SQL on an incremental run — 3 statements:

```sql
-- [1/3] make the merge source table
CREATE OR REPLACE TABLE example_3_database.derived.customer_order_summary__clair_new_rows_<run_id> AS (
    select customer_id, count(*) as total_orders, ...
    from example_3_database.refined.orders
    group by customer_id
)

-- [2/3] merge into the target
MERGE INTO example_3_database.derived.customer_order_summary AS target
USING example_3_database.derived.customer_order_summary__clair_new_rows_<run_id> AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    total_orders = source.total_orders,
    total_amount = source.total_amount,
    first_order_at = source.first_order_at,
    last_order_at = source.last_order_at,
    last_updated_at = source.last_updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, total_orders, total_amount, first_order_at, last_order_at, last_updated_at)
    VALUES (source.customer_id, source.total_orders, source.total_amount, source.first_order_at, source.last_order_at, source.last_updated_at)

-- [3/3] drop the merge source table
DROP TABLE IF EXISTS example_3_database.derived.customer_order_summary__clair_new_rows_<run_id>
```

## Your own join conditions

If the join condition is more than an equality of columns, use `join_sql` in place of `primary_key_columns`:

```python
run_config=Incremental(
    strategy=Upsert(
        join_sql="target.customer_id = source.customer_id AND target.region = source.region",
        update_columns=["total_orders", "total_amount"],
    ),
)
```

## Column overrides

By default, MATCH updates all the columns that are not primary keys, and NO MATCH inserts all the columns. Give `update_columns` or `insert_columns` to `Upsert` to change this:

```python
run_config=Incremental(
    strategy=Upsert(
        primary_key_columns=["customer_id"],
        update_columns=["total_orders", "last_order_at"],
        insert_columns=["customer_id", "total_orders", "total_amount", "last_order_at"],
    ),
)
```

## `DeleteInsert`

`DeleteInsert` deletes each target row that a new row matches, then inserts all the new rows. Use it to replace a window of data, for example the last 3 days.

Choose `DeleteInsert` in place of `Upsert` for two reasons:

- A MERGE fails when two new rows hold the same key. `DeleteInsert` inserts each new row, thus a duplicate key does not stop the run.
- A key on a date column replaces each day that the new rows hold. One delete of a full day costs less than a match of each row.

```python
# example_3_database/derived/daily_orders.py
from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import (
    Column,
    ColumnType,
    DeleteInsert,
    Incremental,
    RunMode,
    Table,
    TestUnique,
)

sql = f"""
    select
        created_date,
        count(*)    as order_count,
        sum(amount) as revenue
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_date >= dateadd('day', -3, current_date())
    """
sql += """
    group by created_date
"""

trouve = Table(
    docs="""
        The orders and the revenue of each day. In each incremental run, clair calculates
        the last 3 days again and replaces them. Thus a late order changes the total of its
        day, and the days before the window stay.
    """,
    sql=sql,
    run_config=Incremental(strategy=DeleteInsert(primary_key_columns=["created_date"])),
    columns=[
        Column(name="created_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.INTEGER),
        Column(name="revenue", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="created_date")],
)
```

The key is `created_date`, thus each incremental run replaces each day of the last 3 days. A day before the window keeps its rows. `DeleteInsert` needs no `columns`: the INSERT copies each column of the new rows.

Generated SQL on an incremental run — 3 statements and 1 cleanup:

```sql
-- [1/4] create the new rows table
CREATE OR REPLACE TABLE example_3_database.derived.daily_orders__clair_new_rows_<run_id> AS (
    select created_date, count(*) as order_count, sum(amount) as revenue
    from example_3_database.refined.orders
    where created_date >= dateadd('day', -3, current_date())
    group by created_date
)

-- [2/4] delete each target row that a new row matches
DELETE FROM example_3_database.derived.daily_orders AS target
USING example_3_database.derived.daily_orders__clair_new_rows_<run_id> AS source
WHERE target.created_date = source.created_date

-- [3/4] insert the new rows
INSERT INTO example_3_database.derived.daily_orders
SELECT * FROM example_3_database.derived.daily_orders__clair_new_rows_<run_id>

-- [4/4] drop the new rows table
DROP TABLE IF EXISTS example_3_database.derived.daily_orders__clair_new_rows_<run_id>
```

The DELETE and the INSERT write to the staging table, and clair promotes it after the tests pass. Thus the physical table never shows the rows between the DELETE and the INSERT. Clair runs the drop of the new rows table after a failure too.

`DeleteInsert` also takes `join_sql`. It gives the `WHERE` condition of the DELETE, with the aliases `target` and `source`.

!!! note
    A day that the new rows do not hold keeps its old rows. If the source deletes all the orders of one day in the window, the old total of that day stays. Use a full refresh to remove it.

## Behavior of the first run

On the first run, the target table does not exist. clair then does a full refresh, and it ignores your strategy. The runs after the first one use your incremental mode.

A Trouve that changes from a `VIEW` to an incremental `TABLE` has no base table either, because the address holds a view. Clair builds and tests a full refresh candidate, then stops before the promotion, and tells you to drop the view — see [A Trouve that changes its type](staging.md#a-trouve-that-changes-its-type).

## Change the mode at the command line

Force a full refresh for one run:

```bash
clair run --env=dev --run-mode=full_refresh
```

## The `run_config` types

`run_config` is `FullRefresh()` or `Incremental(strategy=...)`. Each strategy takes only the fields that it reads: `Append()` takes none, `Upsert` takes the match and the column lists, and `DeleteInsert` takes the match. See the [`RunConfig` API reference](../reference/run-config-api.md) for each field.
