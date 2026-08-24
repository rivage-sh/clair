# Example 3 — Incremental Materializations

Integration test project for `run_mode` functionality. Demonstrates both incremental strategies:
`APPEND` and `UPSERT`.

## Pipeline

```
source.orders  (SOURCE)
    └── refined.orders             (TABLE, full refresh)
            ├── derived.recent_orders          (TABLE, INCREMENTAL APPEND)
            └── derived.customer_order_summary (TABLE, INCREMENTAL UPSERT)
```

| Trouve | Strategy | Description |
|---|---|---|
| `source.orders` | SOURCE | Raw orders table |
| `refined.orders` | Full refresh | Typed + date columns added |
| `derived.recent_orders` | Incremental APPEND | Appends orders created in last 3 days |
| `derived.customer_order_summary` | Incremental UPSERT on `customer_id` | Per-customer stats, merged in-place |

## Prerequisites

- Snowflake account with an environment at `~/.clair/environments.yml` (examples below use `dev`)
- clair installed: from the repo root run `uv sync && source .venv/bin/activate`

## Setup: create the source table

Run this in Snowflake to create and seed `example_3_database.source.orders`:

```sql
create database if not exists example_3_database;
create schema if not exists example_3_database.source;

create or replace table example_3_database.source.orders as
select order_id, customer_id, order_status, amount, created_at, updated_at
from values
    ('ord_001', 'cust_a', 'delivered', 49.99,  dateadd('day', -10, current_timestamp()), dateadd('day', -8,  current_timestamp())),
    ('ord_002', 'cust_b', 'delivered', 120.00, dateadd('day', -9,  current_timestamp()), dateadd('day', -7,  current_timestamp())),
    ('ord_003', 'cust_a', 'delivered', 35.50,  dateadd('day', -7,  current_timestamp()), dateadd('day', -6,  current_timestamp())),
    ('ord_004', 'cust_c', 'shipped',   89.00,  dateadd('day', -3,  current_timestamp()), dateadd('day', -2,  current_timestamp())),
    ('ord_005', 'cust_b', 'placed',    15.00,  dateadd('day', -1,  current_timestamp()), dateadd('day', -1,  current_timestamp())),
    ('ord_006', 'cust_a', 'placed',    200.00, dateadd('day', -1,  current_timestamp()), dateadd('day', -1,  current_timestamp()))
as t(order_id, customer_id, order_status, amount, created_at, updated_at);
```

## Routing

`__routing__.py` at the root of this project holds the routing rules. It has two entries:

| Environment | Physical write target |
|---|---|
| `dev` | `<CLAIR_USER>` — the user name replaces the database |
| `prod` | `example_3_database` — the logical names |

The `dev` entry reads the `CLAIR_USER` environment variable, thus each person writes to a
separate database. Set it before you run the `dev` environment:

```bash
export CLAIR_USER=alice
```

`clair validate` runs each rule on every Trouve and needs no Snowflake connection. It finds a
rule that gives an invalid name, and two Trouves that go to one target:

```bash
clair validate --project examples/projects/example_3
clair validate --project examples/projects/example_3 --env prod
```

This entry gives a SOURCE back unchanged, thus the `source` schema keeps its logical name
in every environment.

See the [routing guide](../../site_docs/docs/guides/routing.md) for the full rules.

## Running the example

All commands run from the repo root (`clair/`).

### 1. Compile (no Snowflake connection)

```bash
# Preview full-refresh SQL (default)
clair compile --project examples/projects/example_3

# Preview incremental SQL — shows INSERT INTO and MERGE statements with <run_id> placeholder
clair compile --project examples/projects/example_3 --run-mode incremental
```

### 2. Full refresh run (first run)

Creates all tables from scratch. Use this to initialise the derived tables before testing incremental.

```bash
clair run --project examples/projects/example_3 --env dev
```

Verify:

```sql
select * from example_3_database.derived.recent_orders order by created_at;
select * from example_3_database.derived.customer_order_summary order by customer_id;
```

### 3. Insert new source data

Simulate new orders arriving:

```sql
insert into example_3_database.source.orders values
    ('ord_007', 'cust_c', 'placed',   55.00, current_timestamp(), current_timestamp()),
    ('ord_008', 'cust_d', 'placed',   310.00, current_timestamp(), current_timestamp());
```

### 4. Incremental run

```bash
clair run --project examples/projects/example_3 --env dev --run-mode incremental
```

Expected behaviour:
- `refined.orders` — full refresh (no `run_config` set), rebuilds from source
- `derived.recent_orders` — **APPEND**: the two new orders are inserted alongside existing rows
- `derived.customer_order_summary` — **UPSERT**: `cust_c` stats are updated; `cust_d` is inserted as a new row

Verify:

```sql
-- recent_orders should now include ord_007 and ord_008
select * from example_3_database.derived.recent_orders order by created_at desc;

-- cust_c total_orders should be 2; cust_d should appear for the first time
select * from example_3_database.derived.customer_order_summary order by customer_id;
```

### 5. Verify UPSERT staging table cleanup

The staging table (`example_3_database.derived.customer_order_summary__clair_staging_<run_id>`)
is created and dropped within the same run. After the run completes it should not exist:

```sql
-- should return no rows
select table_name
from example_3_database.information_schema.tables
where table_name ilike '%clair_staging%';
```

## Teardown

```sql
drop database if exists example_3_database;
```
