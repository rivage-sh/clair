# Project Layout

## Directory → Snowflake name

The directory structure under your project root maps directly to Snowflake fully-qualified names:

```
<project_root>/
  <database>/
    <schema>/
      <table>.py   →   database.schema.table
```

A file at `my_project/refined/products/catalog.py` becomes `refined.products.catalog` in Snowflake.

## Typical layout

A production clair project typically has 3–4 layers:

```
my_project/
├── source/                          # Pre-existing tables — TrouveType.SOURCE
│   ├── orders/
│   │   ├── raw.py                   # source.orders.raw
│   │   └── customers.py             # source.orders.customers
│   └── products/
│       └── catalog.py               # source.products.catalog
│
├── refined/                         # Cleaned, typed, deduplicated
│   ├── orders/
│   │   ├── daily.py                 # refined.orders.daily
│   │   └── returns.py               # refined.orders.returns
│   └── products/
│       └── catalog.py               # refined.products.catalog
│
├── derived/                         # Business-level aggregations
│   └── products/
│       └── top_sellers.py           # derived.products.top_sellers
│
└── reports/                         # Final views for BI tools
    └── products/
        └── summary.py               # reports.products.summary
```

## Special files

| File | Location | Purpose |
|------|----------|---------|
| `__database_config__.py` | database directory | Warehouse/role defaults for all Trouves in that database |
| `__schema_config__.py` | schema directory | Warehouse/role defaults for all Trouves in that schema |

See [Per-Database & Schema Config](../guides/per-database-schema-config.md) for details.

## Files starting with `_`

Any file or directory whose name starts with `_` is skipped by discovery. Use this for shared utilities or helper modules that shouldn't be Trouves:

```
my_project/
└── refined/
    └── orders/
        ├── _utils.py       # ignored by discovery — import freely
        └── daily.py        # discovered as refined.orders.daily
```

## `_clairtifacts/`

Compiled SQL artifacts are written here. Add it to `.gitignore`:

```
# .gitignore
/_clairtifacts
```

## Imports across databases

Python imports work normally. A Trouve in `refined/` can import from `source/`:

```python
# refined/orders/daily.py
from source.orders.raw import trouve as raw_orders
```

clair resolves `source.orders.raw` to the Snowflake object at `source.orders.raw` (subject to any active [routing policy](../guides/routing.md)).
