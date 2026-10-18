# Per-Database & Schema Config

You can set the warehouse, the role, the query tag labels and the statement timeout for each database directory, each schema directory, or one Trouve. Then different parts of your project can use different compute or permissions. You do not change `~/.clair/environments.yml`.

clair reads the two files from the directories of the Trouve: the schema directory is the
parent of the Trouve file, and the database directory is the parent of the schema
directory. Thus the config applies at each depth, and a project below other directories
keeps its defaults.

## `__database_config__.py`

Place this file at the database directory level:

```
my_project/
└── reports/
    ├── __database_config__.py    ← applies to all Trouves under reports/
    └── orders/
        └── summary.py
```

```python
# reports/__database_config__.py
from clair import DatabaseDefaults

defaults = DatabaseDefaults(
    warehouse="reporting_wh",   # a larger warehouse for the BI queries
    role="reporter",
)
```

## `__schema_config__.py`

Place this file at the schema directory level to override the database config for that schema:

```
my_project/
└── reports/
    ├── __database_config__.py
    └── orders/
        ├── __schema_config__.py    ← applies to all Trouves under reports/orders/
        └── summary.py
```

```python
# reports/orders/__schema_config__.py
from clair import SchemaDefaults

defaults = SchemaDefaults(
    warehouse="orders_wh",  # applies to this schema only
)
```

## The Trouve

A Trouve takes the same fields. Use it for one heavy Trouve in a schema of light ones:

```python
trouve = Table(
    sql=f"SELECT ... FROM {events}",
    warehouse="large_wh",
    statement_timeout_seconds=7200,
)
```

## Resolution order

clair resolves the session of each Trouve in this order. A later value replaces an earlier value.

1. The environment defaults: `warehouse` and `role` from `~/.clair/environments.yml`
2. `__database_config__.py` in the database directory of the Trouve
3. `__schema_config__.py` in the schema directory of the Trouve
4. The Trouve

The `query_tags` labels merge by key: a lower level replaces the same key, and keeps the
other keys.

## Field reference

`DatabaseDefaults`, `SchemaDefaults` and each Trouve have the same fields:

| Field | Type | Description |
|-------|------|-------------|
| `warehouse` | `str \| None` | The Snowflake warehouse |
| `role` | `str \| None` | The Snowflake role |
| `query_tags` | `dict[str, str]` | Labels that clair adds to the query tag. See [The query tag](#the-query-tag). |
| `statement_timeout_seconds` | `int \| None` | Snowflake cancels a statement that runs longer. `None` keeps the value of the account or the user. |

If you do not set a field, it stays `None`. clair then uses the value from the level above.

## The query tag

clair sets `QUERY_TAG` before the statements of each Trouve. The tag is JSON:

```json
{"app": "clair", "clair_version": "0.4.0", "environment": "prod", "run_id": "0195...", "team": "finance", "trouve": "reports.orders.summary"}
```

Thus `QUERY_HISTORY` groups the cost by run, by environment and by Trouve, and you
configure nothing. A cost report reads it with `TRY_PARSE_JSON(query_tag):trouve`. Add a
label with `query_tags={"team": "finance"}`. The keys `app`, `clair_version`,
`environment`, `run_id` and `trouve` belong to clair, and a label that uses one raises
`ValueError`.

## Example: mixed warehouses

```
my_project/
├── source/
│   └── __database_config__.py    # defaults = DatabaseDefaults(warehouse="ingest_wh", role="loader")
├── refined/
│   └── __database_config__.py    # defaults = DatabaseDefaults(warehouse="transform_wh", role="transformer")
└── reports/
    └── __database_config__.py    # defaults = DatabaseDefaults(warehouse="reporting_wh", role="reporter")
```

Each database runs on the correct compute. You do not give flags on the command line.
