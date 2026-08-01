# Pandas-Native Transformations

A `PandasTrouve` lets you write a pipeline step as a plain Python function. Clair fetches your upstream tables from Snowflake as DataFrames, runs your function on the machine executing clair, and writes the result back to Snowflake — with full DAG integration, lineage, selectors, and data quality tests.

## When to use `PandasTrouve`

Use it when SQL is the wrong tool for the job:

- Complex reshaping that would require many CTEs
- ML feature engineering
- Multi-step aggregations that depend on intermediate Python state
- Logic you already have as pandas code

For everything else, use a `Trouve` with a `sql` string — it runs entirely inside Snowflake and does not move data over the network.

## Installation

`PandasTrouve` needs no extra installation. pandas is a dependency of clair.

## Basic example

```python
# derived/products/top_rated.py  →  derived.products.top_rated
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import PandasTrouve


def top_rated(catalog: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    merged = catalog.merge(reviews, on="product_id")
    return (
        merged.groupby(["product_id", "name"], as_index=False)
        .agg(avg_rating=("rating", "mean"))
        .query("avg_rating >= 4")
    )


trouve = PandasTrouve(
    transform=top_rated,
    inputs=[catalog_trouve, reviews_trouve],
)
```

Clair binds `inputs` to the parameters of `transform` **by position**. The first Trouve in `inputs` becomes the first parameter, the second becomes the second parameter, and so on. The parameter names are yours to choose — they do not need to match the import names.

Because `transform` is an ordinary function that takes DataFrames, you can call it directly in a unit test or in a notebook:

```python
from derived.products.top_rated import top_rated

result = top_rated(my_catalog_dataframe, my_reviews_dataframe)
```

## With columns and tests

`columns` and `tests` work the same as they do for SQL Trouves:

```python
from clair import Column, ColumnType, PandasTrouve, TestNotNull, TestRowCount

trouve = PandasTrouve(
    transform=top_rated,
    inputs=[catalog_trouve, reviews_trouve],
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

`clair run` gives a `PandasTrouve` these four steps:

1. **Fetch** — for each Trouve in `inputs`, run `SELECT * FROM <full_name>` and load the result into a DataFrame. Column names become lowercase.
2. **Transform** — call your function locally on the clair machine, with one DataFrame for each parameter, in the order of `inputs`.
3. **Write** — write the returned DataFrame back to Snowflake. The table is created or replaced.
4. **Test** — run the attached tests against the output table in Snowflake.

If your function returns something other than a `DataFrame`, the run fails with a clear message and the downstream nodes are skipped.

!!! note
    Clair reads the data into memory on the machine that runs clair. For large upstream tables this is slow and it uses much memory. Chunked reads are not available.

## Validation

Clair examines the `transform` signature when Python loads your file. Thus a mistake stops the run immediately, and it names the fault:

- The count of `inputs` must equal the count of parameters. An error tells you both counts and lists the parameter names.
- The transform must not use `*args` or `**kwargs`. Clair binds each input to a named parameter.

## DAG integration

Dependencies come from `inputs`. No extra configuration is necessary. `clair dag` marks these nodes with a `[PANDAS]` tag, in place of the `[TABLE]` or `[VIEW]` tag:

```
=== Clair DAG: 3 models, 1 source ===

example_4_database.source.events  [SOURCE]
└── example_4_database.refined.events  [TABLE]
    └── example_4_database.derived.daily_event_counts  [PANDAS]
        └── example_4_database.derived.top_event_types  [TABLE]
```

A SQL `Trouve` can depend on the output of a `PandasTrouve`, and a `PandasTrouve` can depend on other `PandasTrouve` nodes. The tree above shows both: a pandas node reads a SQL table, and a SQL table reads the pandas output.

## Selectors

`--select` filtering operates the same way:

```bash
clair run --project=. --env=dev --select='derived.products.top_rated'
```

## Compile output

`clair compile` writes a `.py` artifact for a `PandasTrouve`, in place of the `.sql` file it writes for a SQL Trouve. The artifact holds a header, the imports of the source module, and the source of your function. The header shows which upstream Trouve clair binds to each parameter:

```python
# clair compiled: derived.products.top_rated
# execution_type: pandas
# inputs:
#   catalog  ->  refined.products.catalog
#   reviews  ->  refined.products.reviews

import pandas as pd

def top_rated(catalog: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    ...
```

## Limitations

- **Full-refresh only.** Incremental strategies are not available. A `PandasTrouve` always replaces the table. A `RunConfig` with an incremental mode raises an error.
- **TABLE output only.** Views and sources are not available.
- **Full table fetch.** Clair reads all upstream rows into memory. Chunking is not available.

## Field reference

These are the `PandasTrouve` fields. See the [Trouve API reference](../reference/trouve-api.md) for the full list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `transform` | `Callable[..., pd.DataFrame]` | required | Python function that gives the output DataFrame. |
| `inputs` | `list[TrouveAbc]` | `[]` | The upstream Trouves. Clair binds them to the transform parameters by position. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional — used for documentation. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests, run after clair writes the output. |
| `docs` | `str` | `""` | Documentation string shown in `clair docs`. |

## Complete example

`example_projects/example_4/` in the repository is a runnable project that uses `PandasTrouve`. See `example_4_database/derived/daily_event_counts.py`.
