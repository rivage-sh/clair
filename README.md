<p align="center">
  <img src="docs/assets/clair_logo.png" alt="clair logo" width="200" />
</p>

# clair

Python-native data transformations for Snowflake.

Define your pipelines as Python files. Clair compiles them to SQL, builds a DAG, and runs them against Snowflake in dependency order — with data quality tests, incremental strategies, and structured logging out of the box.

---

## Why clair?

SQL-only tools make it hard to express dependencies clearly. With clair, each table or view is a Python file that references its upstreams by import. And if SQL isn't the right tool — write pandas instead:

```python
from clair import Trouve
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Trouve(
    sql=f"""
        SELECT
            c.product_id,
            c.name,
            avg(r.rating)  AS avg_rating,
            count(*)       AS review_count
        FROM {catalog} c
        JOIN {reviews} r ON c.product_id = r.product_id
        GROUP BY 1, 2
    """,
)
```

Import the upstream, use it in the f-string — clair figures out the rest.

Or skip SQL entirely and write pandas:

```python
import pandas as pd
from clair import PandasTrouve
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

def summarize(inputs):
    df = inputs["catalog"].merge(inputs["reviews"], on="product_id")
    return df.groupby("name")["rating"].mean().reset_index()

trouve = PandasTrouve(
    inputs={"catalog": catalog, "reviews": reviews},
    transform=summarize,
)
```

clair fetches the upstream tables from Snowflake, runs your function locally, then writes the result back. The DAG, lineage, `--select` filters, and data quality tests all work unchanged.

---

## Concepts

### Trouve

A **Trouve** is a single Python file representing one queryable Snowflake object. Its position in the directory tree determines its fully-qualified name:

```
my_project/
└── source/
    └── products/
        └── reviews.py   →   source.products.reviews
```

There are two kinds of Trouve:

- **`Trouve`** — SQL-based. Compiles to a Snowflake `TABLE` or `VIEW`. Runs inside Snowflake.
- **`PandasTrouve`** — pandas-based. Runs a Python function on the machine executing clair, then writes the result back to Snowflake.

`Trouve` has three types:

| Type | What it is |
|------|-----------|
| `SOURCE` | A Snowflake table that contains unprocessed data loaded into Snowflake by external tools. |
| `TABLE` | A Snowflake table that clair creates and populates. |
| `VIEW` | A Snowflake view that clair creates. |

### DAG

Clair discovers all Trouves in your project, resolves the Python import references into a dependency graph, and executes nodes in topological order. If a node fails, all its downstream dependents are skipped automatically.

### Project layout

Each directory level maps to one part of the Snowflake name: `database_name / schema_name / table_name`.

```
my_project/
├── source/
│   └── products/
│       ├── catalog.py             # source.products.catalog
│       └── reviews.py             # source.products.reviews
├── refined/
│   └── products/
│       ├── catalog.py             # refined.products.catalog
│       └── reviews.py             # refined.products.reviews
├── derived/
│   └── products/
│       └── top_reviewed.py        # derived.products.top_reviewed
└── _clairtifacts/                 # compiled SQL artifacts — add to .gitignore
```

Files beginning with `_` are ignored during discovery.

---

## Installation

Clair is not yet on PyPI. Install it as a global CLI tool directly from GitHub using [uv](https://github.com/astral-sh/uv):

```bash
uv tool install git+https://github.com/rivage-sh/clair.git
```

---

## Getting started

### 1. Set up an environment

Run `clair init` — it will prompt for your Snowflake connection details and write `~/.clair/environments.yml`.

### 2. Create a project

```
my_project/
├── source/
│   └── products/
│       ├── catalog.py
│       └── reviews.py
├── refined/
│   └── products/
│       ├── catalog.py
│       └── reviews.py
└── derived/
    └── products/
        └── top_reviewed.py
```

**`source/products/catalog.py`** — declares a pre-existing Snowflake table
```python
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    columns=[
        Column(name="product_id",   type=ColumnType.STRING),
        Column(name="name",         type=ColumnType.STRING),
        Column(name="category",     type=ColumnType.STRING),
        Column(name="price",        type=ColumnType.FLOAT),
    ],
)
```

**`refined/products/catalog.py`** — cleans and standardises the source
```python
from clair import Trouve, TrouveType
from source.products.catalog import trouve as source_catalog

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            product_id,
            initcap(name)     AS name,
            lower(category)   AS category,
            price
        FROM {source_catalog}
        WHERE product_id IS NOT NULL
    """,
)
```

**`refined/products/reviews.py`** — cleans the reviews source
```python
from clair import Trouve, TrouveType
from source.products.reviews import trouve as source_reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            review_id,
            product_id,
            rating,
            review_date
        FROM {source_reviews}
        WHERE rating BETWEEN 1 AND 5
    """,
)
```

**`derived/products/top_reviewed.py`** — joins and aggregates the refined layer
```python
from clair import Trouve, TrouveType
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            c.product_id,
            c.name,
            c.category,
            avg(r.rating)  AS avg_rating,
            count(*)       AS review_count
        FROM {catalog} c
        JOIN {reviews} r ON c.product_id = r.product_id
        GROUP BY 1, 2, 3
        HAVING avg_rating >= 4
    """,
)
```

### 3. Compile and run

```bash
# Inspect the generated SQL without touching Snowflake
clair compile --project=my_project

# Execute against Snowflake
clair run --project=my_project --env=dev
```

---

## CLI reference

```bash
clair --help   # list all commands
```

### `clair init`
Scaffold a new project and write a profile interactively.
```bash
clair init --project=./my_project
```

### `clair compile`
Resolve the DAG and write SQL to `_clairtifacts/`. No Snowflake connection needed.
```bash
clair compile --project=./my_project [--select='db.schema.*'] [--run-mode=incremental]
```

### `clair run`
Execute Trouves against Snowflake in topological order.
```bash
clair run --project=./my_project --env=dev [--select='db.schema.*'] [--run-mode=incremental]
```

### `clair test`
Run data quality tests against Snowflake.
```bash
clair test --project=./my_project --env=dev [--select='db.schema.*']
```

### `clair dag`
Print the dependency graph as an indented tree.
```bash
clair dag --project=./my_project [--select='db.schema.*']
```

### `clair docs`
Start a local web UI showing the project DAG and per-Trouve documentation. No Snowflake connection needed.
```bash
clair docs --project=./my_project [--port=8741] [--host=127.0.0.1] [--no-browser]
```

### `clair clean`
Remove compiled artifacts from `_clairtifacts/`.
```bash
clair clean --project=./my_project --before=7d          # older than 7 days
clair clean --project=./my_project --before=2026-03-01  # before a date
clair clean --project=./my_project --dry-run            # preview only
```

---

## Data quality tests

Attach tests to any TABLE or VIEW Trouve. They run against the live table in Snowflake.

```python
from clair import Trouve, TrouveType, TestUnique, TestNotNull, TestRowCount

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT event_id, user_id, event_type FROM {source_events}",
    tests=[
        TestUnique(column="event_id"),
        TestNotNull(column="user_id"),
        TestRowCount(min_rows=1),
    ],
)
```

| Test | Checks |
|------|--------|
| `TestUnique(column)` | No duplicate values in the column |
| `TestNotNull(column)` | No NULL values in the column |
| `TestRowCount(min_rows, max_rows)` | Row count is within the specified bounds |
| `TestUniqueColumns(columns)` | No duplicate combinations across multiple columns |
| `TestSql(sql)` | Arbitrary SQL — zero returned rows means pass |

`TestSql` lets you write any SQL assertion. Use `THIS` as a placeholder for the owning Trouve's table name, and reference other Trouves via import exactly as you would in `Trouve.sql`:

```python
from clair import THIS, TestSql, Trouve, TrouveType
from source.payments.transactions import trouve as transactions

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT * FROM {transactions}",
    tests=[
        TestSql(sql=f"SELECT * FROM {THIS} WHERE amount < 0"),
        TestSql(sql=f"SELECT * FROM {THIS} t LEFT JOIN {transactions} tx ON t.id = tx.id WHERE tx.id IS NULL"),
    ],
)
```

`TestSql` is skipped during `--sample` runs.

```bash
clair test --project=./my_project --env=dev
```

---

## Incrementality

By default, every run does a full refresh. To process only new data, configure a Trouve with `RunMode.INCREMENTAL` and choose a strategy:

**Append** — inserts new rows, never updates existing ones:
```python
from clair import Trouve, TrouveType, RunConfig, RunMode, IncrementalMode

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT * FROM {source_events}
        WHERE occurred_at > dateadd('day', -3, current_timestamp())
    """,
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.APPEND,
    ),
)
```

**Upsert** — merges on primary key columns, updating existing rows and inserting new ones:
```python
run_config=RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    primary_key_columns=["user_id"],
)
```

If the target table doesn't exist yet, clair runs a full refresh automatically regardless of the configured strategy.

Pass `--run-mode full_refresh` on the CLI to force a full rebuild of everything in a single run.

---

## Pandas-native transformations

When SQL isn't the right tool — complex reshaping, ML feature engineering, multi-step aggregations — use `PandasTrouve`. Your function receives upstream tables as DataFrames, runs locally on the machine executing clair, and the result is written back to Snowflake automatically.

```python
import pandas as pd
from clair import PandasTrouve, Column, ColumnType, TestNotNull
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

def top_rated(inputs):
    df = inputs["catalog"].merge(inputs["reviews"], on="product_id")
    return (
        df.groupby(["product_id", "name"])["rating"]
        .mean()
        .reset_index()
        .rename(columns={"rating": "avg_rating"})
        .query("avg_rating >= 4")
    )

trouve = PandasTrouve(
    inputs={"catalog": catalog, "reviews": reviews},
    transform=top_rated,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="name",       type=ColumnType.STRING),
        Column(name="avg_rating", type=ColumnType.FLOAT),
    ],
    tests=[TestNotNull(column="product_id")],
    docs="Top-rated products by average review score, computed in pandas.",
)
```

Requires the pandas extra:

```bash
uv tool install "clair[pandas] @ git+https://github.com/rivage-sh/clair.git"
```

> **Note:** pandas transformations run on the machine executing clair, not inside Snowflake. Keep this in mind for large tables.

---

## Selectors

All commands accept `--select` for glob-style filtering on fully-qualified names. Repeat the flag to union multiple patterns:

```bash
# Run everything in one schema
clair run --project=. --env=dev --select='source.products.*'

# Run a specific table
clair run --project=. --env=dev --select='derived.products.top_reviewed'

# Compile only tables matching a name pattern across all schemas
clair compile --project=. --select='*.*.top_*'

# Union multiple patterns -- select from two schemas at once
clair run --project=. --env=dev --select='source.products.*' --select='derived.products.*'
```

---

## Per-database and per-schema config

Override the warehouse or role for a specific database or schema:

**`my_db/__database_config__.py`**
```python
from clair import DatabaseDefaults

defaults = DatabaseDefaults(warehouse="transform_wh", role="transformer")
```

**`my_db/derived/__schema_config__.py`**
```python
from clair import SchemaDefaults

defaults = SchemaDefaults(warehouse="reporting_wh")
```

Resolution order (later wins): environment defaults → `__database_config__` → `__schema_config__`.

---

## Example projects

Example projects are included under `example_projects/`:

| Project | What it demonstrates |
|---------|---------------------|
| `example_1` | A minimal 4-Trouve events pipeline with VARIANT flattening |
| `example_2` | A 50-Trouve e-commerce warehouse across 4 layers (source → refined → derived → reports) |
| `example_3` | Incremental APPEND and UPSERT strategies |

Each includes a `setup.sql` to create and seed the source tables and a `verify.sql` to inspect the results.
