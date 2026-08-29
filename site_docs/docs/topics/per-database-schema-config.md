# Per-Database & Schema Config

You can set warehouse and role defaults for each database directory or schema directory. Then different parts of your project can use different compute or permissions. You do not change `~/.clair/environments.yml`.

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

## Resolution order

clair resolves the warehouse and the role of each Trouve in this order. A later value replaces an earlier value.

1. The environment defaults: `warehouse` and `role` from `~/.clair/environments.yml`
2. `__database_config__.py` in the database directory of the Trouve
3. `__schema_config__.py` in the schema directory of the Trouve

A `SchemaDefaults` value overrides a `DatabaseDefaults` value for the same field.

## Field reference

`DatabaseDefaults` and `SchemaDefaults` have the same fields:

| Field | Type | Description |
|-------|------|-------------|
| `warehouse` | `str \| None` | The Snowflake warehouse for this directory |
| `role` | `str \| None` | The Snowflake role for this directory |

If you do not set a field, it stays `None`. clair then uses the value from the next level.

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
