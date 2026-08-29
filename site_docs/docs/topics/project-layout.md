# Project Layout

## `__clair_project__.py` marks the root

Every clair project holds one `__clair_project__.py` file at its root:

```python
# my_project/__clair_project__.py
from clair import ProjectConfig

project = ProjectConfig()
```

`clair init` writes the file. You edit it only for a project inside a Python
package — see [A project inside a Python package](#a-project-inside-a-python-package).

The file gives clair three answers.

**Where the project starts.** A clair command walks up from your working directory to the
first `__clair_project__.py`, in the same way that git finds `.git`. Thus you run
`clair run` from any directory of the project:

```bash
cd my_project/refined/orders
clair run                       # clair finds my_project/
```

`--project` stays as the override. CI and a script give it an exact path:

```bash
clair run --project ~/repos/analytics
```

**Where the project stops.** A directory that holds many projects holds no marker file,
thus clair raises `ProjectMarkerMissingError` and names the directory. Without the marker,
clair reads such a directory as one project, and it builds one DAG from every project
below it.

**How Python names each Trouve module.** See below.

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

## A project inside a Python package

Clair loads each Trouve file as a Python module, and your Trouve files import each other.
The two importers must agree on the name of each module: Python keys `sys.modules` by
name, thus two names for one file give two module objects, two `Trouve` objects, and no
DAG edge between them.

Clair therefore takes the name from `sys.path`. It finds the entry that holds the project
root, and it makes the module name from that entry. That is the name that your own import
gives, thus the two importers agree. A project outside every package keeps the behaviour
that it always had: clair puts the project root on `sys.path`, and `source/orders/raw.py`
becomes `source.orders.raw`.

One layout needs a value in the marker file: an installed package that `sys.path` does not
name, for example an editable install that uses an import finder. Give `package` the
dotted name of the project root:

```python
# monorepo/clair_projects/analytics/__clair_project__.py
from clair import ProjectConfig

project = ProjectConfig(package="clair_projects.analytics")
```

Clair then reads `monorepo/` as the import root, and it names the Trouve files below it
`clair_projects.analytics.source.orders.raw` — the name that this import writes:

```python
# monorepo/clair_projects/analytics/refined/orders/daily.py
from clair_projects.analytics.source.orders.raw import trouve as raw

trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {raw}")
```

If clair loads one file two times, discovery stops. A reference token
(`__CLAIR_TROUVE_...`) then stays in the SQL, and clair never sends such SQL to Snowflake.
The error names the file, both module names, and this `package` value.

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
├── __clair_project__.py             # Marks the project root
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
| `__clair_project__.py` | project root | Marks the root of the project. `clair init` writes it |
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
