# Clair Example Project

A minimal Clair project with four Trouves, intended for integration testing against a real Snowflake account.

## What it does

This project defines a simple pipeline across three layers:

1. **`example_1_database.source.events`** (SOURCE) — references a pre-existing Snowflake table containing raw user interaction events.
2. **`example_1_database.refined.events`** (TABLE) — flattens the semi-structured `PROPERTIES` VARIANT into typed columns and adds `event_date`.
3. **`example_1_database.derived.daily_event_counts`** (TABLE) — aggregates refined events into daily counts per event type.
4. **`example_1_database.derived.top_event_types`** (TABLE) — ranks the top 10 event types by total count.

Lineage: `source.events` → `refined.events` → `derived.daily_event_counts` → `derived.top_event_types`

## Prerequisites

You need a Snowflake account with:

- An environment configured at `~/.clair/environments.yml` (the examples below use `dev`)
- The source table `example_1_database.source.events` created and populated

### Install clair

From the **project root** (`clair/`), run:

```bash
uv sync
source .venv/bin/activate
```

This installs the `clair` CLI into `.venv/bin/clair`. With the venv activated, `clair` is available on your PATH.

### Create the source table

Run this SQL in Snowflake to set up the source table with sample data:

```sql
create database if not exists example_1_database;
create schema if not exists example_1_database.source;

create table example_1_database.source.events as
select
    event_id,
    user_id,
    event_type,
    occurred_at,
    parse_json(properties) as properties
from values
    ('1', 'usr_abc', 'page_view',    '2024-01-15 08:23:11'::timestamp_ntz, '{"page": "/home", "referrer": "google.com"}'),
    ('2', 'usr_abc', 'button_click', '2024-01-15 08:24:05'::timestamp_ntz, '{"element": "signup_btn", "page": "/home"}'),
    ('3', 'usr_def', 'page_view',    '2024-01-15 09:01:33'::timestamp_ntz, '{"page": "/pricing", "referrer": null}'),
    ('4', 'usr_def', 'form_submit',  '2024-01-15 09:03:47'::timestamp_ntz, '{"form": "contact", "success": true}'),
    ('5', 'usr_ghi', 'purchase',     '2024-01-15 11:45:00'::timestamp_ntz, '{"item_id": "prod_99", "amount": 49.99, "currency": "USD"}')
as t(event_id, user_id, event_type, occurred_at, properties);
```

## Routing

`__routing__.py` at the root of this project holds the routing rules. It has two entries:

| Environment | Physical write target |
|---|---|
| `dev` | `<CLAIR_USER>` — the user name replaces the database |
| `prod` | `example_1_database` — the logical names |

The `dev` entry reads the `CLAIR_USER` environment variable, thus each person writes to a
separate database. Set it before you run the `dev` environment:

```bash
export CLAIR_USER=alice
```

`clair validate` runs each rule on every Trouve and needs no Snowflake connection. It finds a
rule that gives an invalid name, and two Trouves that go to one target:

```bash
clair validate --project example_projects/example_1
clair validate --project example_projects/example_1 --env prod
```

SOURCE Trouves never route. The `source` schema keeps its logical name in every environment.

See the [routing guide](../../site_docs/docs/guides/routing.md) for the full rules.

## Running the example

From the project root (`clair/`):

```bash
# Compile (offline -- resolves the DAG and prints the generated SQL)
clair compile --project example_projects/example_1

# Run (executes against Snowflake)
clair run --project example_projects/example_1 --env dev
```

After running, you should see three new tables in Snowflake:

- `example_1_database.refined.events`
- `example_1_database.derived.daily_event_counts`
- `example_1_database.derived.top_event_types`

Verify with:

```sql
select * from example_1_database.refined.events;
select * from example_1_database.derived.daily_event_counts order by event_date desc;
select * from example_1_database.derived.top_event_types;
```
