# Per-Database & Schema Config

You can set warehouse and role defaults per database or schema directory. This lets different parts of your project run with different compute or permissions without touching `~/.clair/environments.yml`.

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
    warehouse="reporting_wh",   # larger warehouse for BI queries
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
    warehouse="orders_wh",  # override for this schema only
)
```

## Resolution order

For each Trouve, the effective warehouse and role are resolved in this order (later values win):

1. Environment defaults (`warehouse` and `role` from `~/.clair/environments.yml`)
2. `__database_config__.py` in the Trouve's database directory
3. `__schema_config__.py` in the Trouve's schema directory

A `SchemaDefaults` value overrides a `DatabaseDefaults` value for the same field.

## Field reference

Both `DatabaseDefaults` and `SchemaDefaults` support the same fields:

| Field | Type | Description |
|-------|------|-------------|
| `warehouse` | `str \| None` | Snowflake warehouse to use for this directory |
| `role` | `str \| None` | Snowflake role to use for this directory |

Fields not set (left as `None`) fall through to the next level in the resolution order.

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

Each database runs on the appropriate compute without any per-command flags.
