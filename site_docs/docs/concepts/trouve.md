# Trouve

A Trouve is the fundamental unit in clair. One Python file = one queryable Snowflake object. Every Trouve is discovered automatically by the framework; you never register them manually.

## Types

| Type | Description | SQL required? |
|------|-------------|---------------|
| `SOURCE` | Pre-existing table managed by an external tool (e.g. Fivetran, Airbyte) | No |
| `TABLE` | clair-managed Snowflake table | Yes |
| `VIEW` | clair-managed Snowflake view | Yes |

## The f-string pattern

The most important thing to understand about Trouves is how they reference each other. When you write an f-string SQL query, you embed another Trouve directly:

```python
from source.products.catalog import trouve as source_catalog

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT * FROM {source_catalog}",
)
```

When `{source_catalog}` is evaluated, it calls `Trouve.__format__`, which registers a placeholder token. During discovery, clair replaces that token with the real Snowflake fully-qualified name (e.g. `source.products.catalog`). The Python import also tells clair about the dependency — no separate DAG configuration is needed.

## Field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `TrouveType` | `TABLE` | SOURCE, TABLE, or VIEW |
| `sql` | `str` | `""` | SQL query. Required for TABLE/VIEW, must be empty for SOURCE. |
| `columns` | `list[Column]` | `[]` | Column definitions. Required for UPSERT mode. See [Column](../reference/column-api.md). |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](../guides/data-quality-tests.md). |
| `docs` | `str` | `""` | Documentation string shown in `clair docs`. |
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [Incrementality](../guides/incrementality.md). |

## Examples

### SOURCE

A pre-existing Snowflake table. No SQL — clair never writes to it.

```python
# source/orders/raw.py  →  source.orders.raw
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw orders table loaded by Fivetran.",
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
```

### TABLE

A clair-managed table. Imports an upstream Trouve and references it in an f-string.

```python
# refined/orders/daily.py  →  refined.orders.daily
from clair import Column, ColumnType, Trouve, TrouveType, TestNotNull
from source.orders.raw import trouve as raw_orders

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"""
        SELECT
            order_id,
            customer_id,
            amount,
            created_at::date AS created_date
        FROM {raw_orders}
        WHERE order_id IS NOT NULL
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING, nullable=False),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_date", type=ColumnType.DATE),
    ],
    tests=[TestNotNull(column="order_id")],
    docs="Cleaned orders with date column extracted.",
)
```

### VIEW

Same as TABLE but creates a `CREATE OR REPLACE VIEW` instead. Does not support incremental strategies.

```python
# reports/orders/recent.py  →  reports.orders.recent
from clair import Trouve, TrouveType
from refined.orders.daily import trouve as daily_orders

trouve = Trouve(
    type=TrouveType.VIEW,
    sql=f"""
        SELECT *
        FROM {daily_orders}
        WHERE created_date >= dateadd('day', -30, current_date())
    """,
)
```

## Pandas execution (`PandasTrouve`)

When SQL is not the right tool, use a `PandasTrouve` in place of a `Trouve`. You supply a Python function. Clair fetches the upstream tables from Snowflake as DataFrames, calls your function on the machine that runs clair, then writes the result back to Snowflake.

```python
# derived/products/top_rated.py
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import PandasTrouve


def top_rated(catalog: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    merged = catalog.merge(reviews, on="product_id")
    return (
        merged.groupby(["product_id", "name"], as_index=False)
        .agg(rating=("rating", "mean"))
        .query("rating >= 4")
    )


trouve = PandasTrouve(
    transform=top_rated,
    inputs=[catalog_trouve, reviews_trouve],
)
```

Clair binds `inputs` to the transform parameters by position. The transform takes plain DataFrames, thus you can call it directly in a test or in a notebook.

Differences between the two backends:

| | `Trouve` | `PandasTrouve` |
|---|---|---|
| Execution | Inside Snowflake | Locally on the clair machine |
| Output type | TABLE or VIEW | TABLE only |
| Incremental | Supported | Full-refresh only |
| Dependencies | f-string references in `sql` | the `inputs` list |

Everything else — DAG integration, `--select` filtering, data quality tests, `clair dag` output — operates the same way. Both classes derive from `TrouveAbc`, the abstract base that holds `columns`, `tests`, `docs`, and `run_config`.

See the [Pandas-native guide](../guides/pandas-native.md) for a full walkthrough.

## After discovery

Once clair discovers a Trouve, it sets `compiled` attributes. Two useful properties become available:

- `trouve.full_name` — the fully-qualified Snowflake name (e.g. `refined.orders.daily`)
- `trouve.is_compiled` — `True` once the project has been discovered

Accessing `full_name` before discovery raises `RuntimeError`.
