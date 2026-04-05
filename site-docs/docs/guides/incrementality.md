# Incrementality

By default, clair rebuilds every Trouve from scratch on each run (`CREATE OR REPLACE TABLE`). For large tables, you can configure incremental strategies that apply only new data.

## Default: full refresh

```python
trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT * FROM {upstream}",
    # No run_config — defaults to full refresh
)
```

Generated SQL:

```sql
CREATE OR REPLACE TABLE example_3_database.refined.orders AS (
    SELECT * FROM example_3_database.source.orders
)
```

## APPEND

APPEND inserts only new rows on each incremental run. Use it for event streams and append-only logs.

```python
# example_3_database/derived/recent_orders.py
from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Incremental append of recent orders. On each incremental run, orders created in the
        last 3 days are appended. Intended to accumulate a running log of recent activity.

        Note: the 3-day lookback provides a small overlap window to handle late-arriving rows.
        On full refresh, this table is rebuilt from scratch using the same SQL.
    """,
    sql=f"""
        select
            order_id,
            customer_id,
            order_status,
            amount,
            created_at,
            created_date
        from {refined_orders}
        where created_at > dateadd('day', -3, current_timestamp())
    """,
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

UPSERT merges new data into the target table, updating existing rows and inserting new ones. Use it for slowly-changing tables (e.g. per-customer aggregates).

```python
# example_3_database/derived/customer_order_summary.py
from clair import Column, ColumnType, IncrementalMode, RunConfig, RunMode, Trouve, TrouveType
from example_3_database.refined.orders import trouve as refined_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="""
        Per-customer order statistics, merged on customer_id.

        On each incremental run, stats are recomputed from all refined orders and merged
        into the target table. Existing customers are updated in-place; new customers are
        inserted. This means the table always reflects current totals without a full rebuild.

        On full refresh, the table is created from scratch with CREATE OR REPLACE.
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
    `columns` is required for UPSERT mode. clair uses the column list to build the MERGE statement.

Generated SQL on an incremental run (3 statements):

```sql
-- [1/3] create staging table
CREATE OR REPLACE TABLE example_3_database.derived.customer_order_summary__clair_staging_<run_id> AS (
    select customer_id, count(*) as total_orders, ...
    from example_3_database.refined.orders
    group by customer_id
)

-- [2/3] merge into target
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

-- [3/3] drop staging table
DROP TABLE IF EXISTS example_3_database.derived.customer_order_summary__clair_staging_<run_id>
```

## Custom join conditions

If the join condition is more complex than a simple column equality, use `join_sql` instead of `primary_key_columns`:

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

By default, all non-primary-key columns are updated on MATCH and all columns are inserted on NO MATCH. Use `UpsertConfig` to override:

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

## First-run behavior

On the very first run (when the target table doesn't exist yet), clair always performs a full refresh regardless of the configured strategy. Subsequent runs use the configured incremental mode.

## Override at runtime

Force a full rebuild for any run:

```bash
clair run --project=. --env=dev --run-mode=full_refresh
```

## `RunConfig` field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_mode` | `RunMode` | `FULL_REFRESH` | `FULL_REFRESH` or `INCREMENTAL` |
| `incremental_mode` | `IncrementalMode \| None` | `None` | Required when `run_mode=INCREMENTAL`: `APPEND` or `UPSERT` |
| `primary_key_columns` | `list[str] \| None` | `None` | UPSERT join key columns (generates `ON target.col = source.col`) |
| `join_sql` | `str \| None` | `None` | Custom `ON` clause for UPSERT (alternative to `primary_key_columns`) |
| `upsert_config` | `UpsertConfig \| None` | `None` | Fine-grained column overrides for MERGE |

See also: [`RunConfig` API reference](../reference/run-config-api.md).
