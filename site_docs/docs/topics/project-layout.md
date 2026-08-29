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

## Why the path is the address

Most tools give a model a name inside the file, or inside a configuration file. clair reads
the name from the path. This is a rule about correctness, and not a matter of taste.

**A duplicate address is impossible to write.** Two Trouves cannot hold one address,
because two files cannot hold one path. Your file system is the name table. clair
maintains none, and it needs no rule to find a collision that you cannot make.

**You read the DAG before you open a file.** `tree` shows the shape of the warehouse. A
reviewer sees a new layer, a new schema, or a table in the wrong database, in the diff of
the file names.

**The configuration inherits, and clair has no configuration language.** A directory is
the scope. `__database_config__.py` applies to each Trouve below it, and
`__schema_config__.py` replaces it for one schema. See
[Per-Database & Schema Config](per-database-schema-config.md). This is why clair has no
`dbt_project.yml`: the tree already says which settings apply where.

**One Trouve has exactly one home.** You look for `refined.orders.daily` at
`refined/orders/daily.py`. There is no second place to look, and no index to read.

The cost is one constraint: one file holds one Trouve, and the file sits three levels below
the project root. clair keeps that constraint on purpose. A Python API that builds a Trouve
with no file must take the address as a field, where a typo makes a collision that nothing
finds, and it must express the directory defaults again as nested objects. The constraint
is worth more than the flexibility.

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
