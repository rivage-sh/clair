# Incrementality

By default, clair makes every Trouve again on each run with `CREATE OR REPLACE TABLE`. For a large table, you can configure an incremental strategy. The strategy applies the new data only.

## Default: full refresh

```python
trouve = Trouve(
    type=TrouveType.TABLE,
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

## APPEND

APPEND adds the new rows only on each incremental run. Use it for event streams and for logs that you add to.

Read `clair.run_mode` to change the SQL between a full refresh and an incremental run:

```python
# example_3_database/derived/recent_orders.py
import clair
from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

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

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Incremental append of the recent orders. In each incremental run, clair adds the
        orders from the last 3 days. Thus the table keeps a log of the recent activity.

        Note: the lookback of 3 days gives a small overlap. The overlap catches the rows that
        come late. In a full refresh, clair selects all rows and uses no date filter.
    """,
    sql=sql,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.APPEND,
    ),
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

## UPSERT

UPSERT merges the new data into the target table. It updates the rows that exist and inserts the new rows. Use it for a table that changes slowly, such as an aggregate for each customer.

```python
# example_3_database/derived/customer_order_summary.py
from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
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
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.UPSERT,
        primary_key_columns=["customer_id"],
    ),
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
    UPSERT mode needs `columns`. clair uses the column list to build the MERGE statement.

Generated SQL on an incremental run — 3 statements:

```sql
-- [1/3] make the staging table
CREATE OR REPLACE TABLE example_3_database.derived.customer_order_summary__clair_staging_<run_id> AS (
    select customer_id, count(*) as total_orders, ...
    from example_3_database.refined.orders
    group by customer_id
)

-- [2/3] merge into the target
MERGE INTO example_3_database.derived.customer_order_summary AS target
USING example_3_database.derived.customer_order_summary__clair_staging_<run_id> AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    total_orders = source.total_orders,
    total_amount = source.total_amount,
    first_order_at = source.first_order_at,
    last_order_at = source.last_order_at,
    last_updated_at = source.last_updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, total_orders, total_amount, first_order_at, last_order_at, last_updated_at)
    VALUES (source.customer_id, source.total_orders, source.total_amount, source.first_order_at, source.last_order_at, source.last_updated_at)

-- [3/3] drop the staging table
DROP TABLE IF EXISTS example_3_database.derived.customer_order_summary__clair_staging_<run_id>
```

## Your own join conditions

If the join condition is more than an equality of columns, use `join_sql` in place of `primary_key_columns`:

```python
run_config=RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    join_sql="target.customer_id = source.customer_id AND target.region = source.region",
    upsert_config=UpsertConfig(
        update_columns=["total_orders", "total_amount"],
    ),
)
```

## Column overrides with `UpsertConfig`

By default, MATCH updates all the columns that are not primary keys, and NO MATCH inserts all the columns. Use `UpsertConfig` to change this:

```python
from clair import UpsertConfig

run_config=RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    primary_key_columns=["customer_id"],
    upsert_config=UpsertConfig(
        update_columns=["total_orders", "last_order_at"],
        insert_columns=["customer_id", "total_orders", "total_amount", "last_order_at"],
    ),
)
```

## Behavior of the first run

On the first run, the target table does not exist. clair then does a full refresh, and it ignores your strategy. The runs after the first one use your incremental mode.

## Change the mode at the command line

Force a full refresh for one run:

```bash
clair run --project=. --env=dev --run-mode=full_refresh
```

## `RunConfig` field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_mode` | `RunMode` | `FULL_REFRESH` | `FULL_REFRESH` or `INCREMENTAL` |
| `incremental_mode` | `IncrementalMode \| None` | `None` | `APPEND` or `UPSERT`. Necessary if `run_mode=INCREMENTAL`. |
| `primary_key_columns` | `list[str] \| None` | `None` | The join columns for UPSERT. clair generates `ON target.col = source.col`. |
| `join_sql` | `str \| None` | `None` | Your own `ON` clause for UPSERT. Use it in place of `primary_key_columns`. |
| `upsert_config` | `UpsertConfig \| None` | `None` | Exact control of the columns in the MERGE |

See also: [`RunConfig` API reference](../reference/run-config-api.md).
