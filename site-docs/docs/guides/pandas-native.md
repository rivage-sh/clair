# Pandas-Native Transformations

`PandasTrouve` lets you write any pipeline step as a plain Python function. Clair fetches your upstream tables from Snowflake as DataFrames, runs your function on the machine executing clair, and writes the result back to Snowflake — with full DAG integration, lineage, selectors, and data quality tests.

## When to use PandasTrouve

Use it when SQL is the wrong tool for the job:

- Complex reshaping that would require many CTEs
- ML feature engineering
- Multi-step aggregations that depend on intermediate Python state
- Logic you already have as pandas code

For everything else, prefer `Trouve` with SQL — it runs entirely inside Snowflake and doesn't require moving data over the network.

## Installation

`PandasTrouve` requires no additional installation — pandas is included in clair by default.

## Basic example

```python
# derived/products/top_rated.py  →  derived.products.top_rated
import pandas as pd
from clair import PandasTrouve
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

def top_rated(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
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
)
```

`inputs` maps string keys to upstream Trouve objects (imported as usual). Those same keys are passed to your function at runtime.

## With columns and tests

`columns` and `tests` work identically to SQL Trouves:

```python
from clair import PandasTrouve, Column, ColumnType, TestNotNull, TestRowCount

trouve = PandasTrouve(
    inputs={"catalog": catalog, "reviews": reviews},
    transform=top_rated,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="name",       type=ColumnType.STRING),
        Column(name="avg_rating", type=ColumnType.FLOAT),
    ],
    tests=[
        TestNotNull(column="product_id"),
        TestRowCount(min_rows=1),
    ],
    docs="Top-rated products by average review score, computed in pandas.",
)
```

## How it runs

When clair encounters a `PandasTrouve` node during `clair run`:

1. **Fetch** — for each entry in `inputs`, executes `SELECT * FROM <full_name>` and loads the result into a DataFrame
2. **Transform** — calls `transform({"key": df, ...})` locally on the clair machine
3. **Write** — writes the returned DataFrame back to Snowflake (`CREATE OR REPLACE TABLE`)
4. **Test** — runs any attached tests against the output table in Snowflake

!!! note
    Data is fetched into memory on the machine running clair. For large upstream tables this can be slow and memory-intensive. Chunked reads are not supported in v1.

## DAG integration

Dependencies are declared via the `inputs` dict — no separate configuration needed. `clair dag` renders `PandasTrouve` nodes with a `[pandas]` annotation:

```
derived.products.top_rated [pandas]
  refined.products.catalog [table]
  refined.products.reviews [table]
```

SQL Trouves can depend on `PandasTrouve` output, and `PandasTrouve` nodes can depend on other `PandasTrouve` nodes.

## Selectors

`--select` filtering works the same way:

```bash
clair run --project=. --env=dev --select='derived.products.top_rated'
```

## Compile output

`clair compile` writes a `.json` manifest for each `PandasTrouve` instead of a `.sql` file:

```json
{
  "type": "pandas",
  "full_name": "derived.products.top_rated",
  "inputs": {"catalog": "refined.products.catalog", "reviews": "refined.products.reviews"},
  "transform_fn": "top_rated",
  "source_file": "derived/products/top_rated.py"
}
```

## Limitations (v1)

- **Full-refresh only.** Incremental strategies are not supported. `PandasTrouve` always runs `CREATE OR REPLACE TABLE`.
- **TABLE output only.** Views are not supported.
- **Full table fetch.** All upstream rows are loaded into memory. There is no chunking.

## Field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inputs` | `dict[str, Trouve]` | required | Maps keys to upstream Trouves. Keys become the keys in the dict passed to `transform`. |
| `transform` | `Callable[[dict[str, DataFrame]], DataFrame]` | required | Python function that receives the input DataFrames and returns the output DataFrame. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional — used for documentation and schema enforcement. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests run after the output is written. |
| `docs` | `str` | `""` | Documentation string shown in `clair docs`. |
