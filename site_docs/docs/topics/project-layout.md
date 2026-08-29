# Project Layout

## Directory → Snowflake name

The directory structure below your project root maps directly to fully-qualified Snowflake names:

```
<project_root>/
  <database>/
    <schema>/
      <table>.py   →   database.schema.table
```

A file at `my_project/refined/products/catalog.py` becomes `refined.products.catalog` in Snowflake.

A Trouve file sits three levels below the project root. A Trouve file above that depth has
no schema directory and no database directory, thus clair cannot make its address. clair
raises `ProjectDiscoveryError` and names the file. The rule applies to a Trouve file only:
a Python file that declares no `trouve` object can sit anywhere, and discovery skips it.

## A project inside a larger repository

The three directories are the three **last** parts of the path. A directory above the
database directory takes no part in the address, thus you can put a clair project below
other directories:

```
monorepo/
└── teams/
    └── analytics/
        └── clair_project/          ← the project root
            └── refined/
                └── products/
                    └── catalog.py  →   refined.products.catalog
```

`__database_config__.py` and `__schema_config__.py` go in the database directory and the
schema directory of the Trouve, at each depth. The directory that the address names is the
directory that holds the config.

One rule applies to the name of a database directory. A Trouve imports a different Trouve
from the project root — `from source.orders.raw import trouve` — so the database directory
must not share its name with a package that Python can already import in your environment.
A package that pip installed wins, and the import of the Trouve then fails with
`No module named 'source.orders'`.

## Typical layout

A clair project in production usually has 3 or 4 layers:

```
my_project/
├── __routing__.py                   # The routing entry of each environment
│
├── source/                          # Tables that already exist — TrouveType.SOURCE
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
| `__routing__.py` | project root | The [routing](routing.md) entry of each environment |
| `__database_config__.py` | database directory | Warehouse/role defaults for all Trouves in that database |
| `__schema_config__.py` | schema directory | Warehouse/role defaults for all Trouves in that schema |

See [Per-Database & Schema Config](per-database-schema-config.md) for details.

## Files starting with `_`

Discovery skips any file or directory with a name that starts with `_`. Use this for shared utilities or helper modules that are not Trouves:

```
my_project/
└── refined/
    └── orders/
        ├── _utils.py       # discovery skips this file — you can import it
        └── daily.py        # discovery finds this as refined.orders.daily
```

## `_clairtifacts/`

clair writes the compiled SQL artifacts here. Add this directory to `.gitignore`:

```
# .gitignore
/_clairtifacts
```

## Imports between databases

Python imports work in the usual way. A Trouve in `refined/` can import from `source/`:

```python
# refined/orders/daily.py
from source.orders.raw import trouve as raw_orders
```

clair resolves `source.orders.raw` to the Snowflake object at `source.orders.raw`. An active [routing entry](routing.md) can change this target.
