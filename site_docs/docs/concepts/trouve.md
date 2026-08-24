# Trouve

A Trouve is the basic unit in clair. One Python file is one Snowflake object that you can query. clair finds every Trouve automatically. You do not register them manually.

## Types

| Type | Description | SQL required? |
|------|-------------|---------------|
| `SOURCE` | A table that already exists. An external tool controls it (e.g. Fivetran, Airbyte). | No |
| `TABLE` | A Snowflake table that clair controls | Yes |
| `VIEW` | A Snowflake view that clair controls | Yes |

## The f-string pattern

Trouves reference each other in one way only. Write an f-string SQL query, then put another Trouve into it:

```python
from source.products.catalog import trouve as source_catalog

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT * FROM {source_catalog}",
)
```

`{source_catalog}` calls `Trouve.__format__`, which registers a placeholder token. At discovery, clair replaces that token with the real fully-qualified Snowflake name, such as `source.products.catalog`. The Python import also tells clair about the dependency. clair does not need a separate DAG configuration.

## Field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `TrouveType` | `TABLE` | SOURCE, TABLE, or VIEW |
| `sql` | `str` | `""` | SQL query. Required for TABLE/VIEW, must be empty for SOURCE. |
| `columns` | `list[Column]` | `[]` | Column definitions. Required for UPSERT mode. See [Column](../reference/column-api.md). |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](../guides/data-quality-tests.md). |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [Incrementality](../guides/incrementality.md). |

## Examples

### SOURCE

A Snowflake table that already exists. No SQL — clair does not write to it.

```python
# source/orders/raw.py  →  source.orders.raw
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="Raw orders table. Fivetran loads it.",
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_at", type=ColumnType.TIMESTAMP_NTZ),
    ],
)
```

### TABLE

A table that clair controls. It imports an upstream Trouve and references it in an f-string.

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
    docs="Cleaned orders with a date column.",
)
```

### VIEW

A VIEW is the same as a TABLE, but clair runs `CREATE OR REPLACE VIEW`. A VIEW cannot use incremental strategies.

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

If SQL is not the correct tool, use a `PandasTrouve` in place of a `Trouve`. You supply a Python function. clair reads the upstream tables from Snowflake as DataFrames. Then it calls your function on the machine that runs clair, and writes the result to Snowflake.

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

clair binds each item of `inputs` to a transform parameter by position. The transform takes plain DataFrames, thus you can call it directly in a test or in a notebook.

The two backends have these differences:

| | `Trouve` | `PandasTrouve` |
|---|---|---|
| Runs | In Snowflake | On the clair machine |
| Output type | TABLE or VIEW | TABLE only |
| Incremental | Yes | Full refresh only |
| Dependencies | f-string references in `sql` | The `inputs` list |

All the other behaviour is the same: the DAG, the `--select` flag, the data quality tests, and the `clair dag` output. The two classes have the same base class, `TrouveAbc`. That base class holds `columns`, `tests`, `docs`, and `run_config`.

See the [Pandas-native guide](../guides/pandas-native.md) for a full example.

## After discovery

After clair discovers a Trouve, it sets the `compiled` attributes. Two properties become available:

- `trouve.physical_name` — the fully-qualified Snowflake name, such as `refined.orders.daily`
- `trouve.is_compiled` — `True` after clair discovers the project

If you read `physical_name` before discovery, clair raises `RuntimeError`.
