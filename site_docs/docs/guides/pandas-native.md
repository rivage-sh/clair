# Pandas-Native Transformations

A `Trouve` with a `df_fn` lets you write a pipeline step as a plain Python function. clair reads your upstream tables from Snowflake as DataFrames. Then it calls your function on the machine that runs clair, and writes the result to Snowflake. The DAG, the lineage, the selectors, and the data quality tests all apply.

## When to use `df_fn`

Use it if SQL is the incorrect tool for the task:

- A complex reshape that needs many CTEs
- ML feature engineering
- Aggregations of many steps that depend on Python state between the steps
- Logic that you already have as pandas code

For all other work, give the `Trouve` a `sql` string. The SQL runs in Snowflake, and the data does not move on the network.

## Installation

`df_fn` needs no extra installation. pandas is a dependency of clair.

## Basic example

```python
# derived/products/top_rated.py  →  derived.products.top_rated
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import Trouve


def top_rated(
    catalog: pd.DataFrame = catalog_trouve,  # type: ignore
    reviews: pd.DataFrame = reviews_trouve,  # type: ignore
) -> pd.DataFrame:
    df = catalog.merge(reviews, on="product_id")
    return (
        df.groupby(["product_id", "name"], as_index=False)["rating"]
        .mean()
        .rename(columns={"rating": "avg_rating"})
        .query("avg_rating >= 4")
    )


trouve = Trouve(df_fn=top_rated)
```

You declare each dependency as a **parameter default value**: annotate the parameter as `pd.DataFrame` and give it the upstream Trouve object as its default. At run time clair replaces each default with the fetched DataFrame and calls your function. The parameter name is the name you use in the function body — it does not need to match the import.

!!! note
    The `# type: ignore` comment is necessary. A type checker sees a `Trouve` object assigned to a `pd.DataFrame` parameter and reports a mismatch. clair substitutes the DataFrame before it calls the function, so the annotation is correct at run time.

## With columns and tests

`columns` and `tests` work the same as they do for SQL Trouves:

```python
from clair import Column, ColumnType, TestNotNull, TestRowCount, Trouve

trouve = Trouve(
    df_fn=top_rated,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="name",       type=ColumnType.STRING),
        Column(name="avg_rating", type=ColumnType.FLOAT),
    ],
    tests=[
        TestNotNull(column="product_id"),
        TestRowCount(min_rows=1),
    ],
    docs="The top-rated products, by the mean review score. pandas calculates them.",
)
```

## How it runs

`clair run` gives a `df_fn` Trouve these four steps:

1. **Fetch** — for each parameter whose default is a Trouve, run `SELECT * FROM <full_name>` and load the result into a DataFrame. Column names become lowercase.
2. **Transform** — call your function locally on the clair machine, with one keyword argument for each parameter.
3. **Write** — write the DataFrame from your function to Snowflake. clair creates or replaces the table.
4. **Test** — run the attached tests against the output table in Snowflake.

If your function returns a different type than `DataFrame`, the run fails with a clear message, and clair skips the downstream nodes.

!!! note
    clair reads the data into the memory of the machine that runs clair. For large upstream tables this is slow, and it uses much memory. Chunked reads are not available.

## DAG integration

Dependencies come from the parameter defaults. No extra configuration is necessary. `clair dag` marks these nodes with a `[PANDAS]` tag, in place of the `[TABLE]` or `[VIEW]` tag:

```
=== Clair DAG: 3 models, 1 source ===

example_4_database.source.events  [SOURCE]
└── example_4_database.refined.events  [TABLE]
    └── example_4_database.derived.daily_event_counts  [PANDAS]
        └── example_4_database.derived.top_event_types  [TABLE]
```

SQL Trouves can depend on the output of a `df_fn` Trouve, and a `df_fn` Trouve can depend on other `df_fn` Trouves. The tree above shows both: a `df_fn` node reads a SQL table, and a SQL table reads the `df_fn` output.

## Selectors

`--select` works in the same way:

```bash
clair run --project=. --env=dev --select='derived.products.top_rated'
```

## Compile output

`clair compile` writes a `.py` artifact for a `df_fn` Trouve, in place of the `.sql` file it writes for a SQL Trouve. The artifact holds a header, the imports of the source module, and the source of your function:

```python
# clair compiled: derived.products.top_rated
# execution_type: pandas
# inputs:
#   catalog  ->  refined.products.catalog
#   reviews  ->  refined.products.reviews

import pandas as pd

def top_rated(
    catalog: pd.DataFrame = catalog_trouve,  # type: ignore
    reviews: pd.DataFrame = reviews_trouve,  # type: ignore
) -> pd.DataFrame:
    ...
```

## Limitations

- **Full-refresh only.** Incremental strategies are not available. A `df_fn` Trouve always replaces the table. A `RunConfig` with an incremental mode raises an error.
- **TABLE output only.** Views are not available.
- **A full table read.** clair reads all the upstream rows into memory. Chunked reads are not available.
- **`sql` and `df_fn` are mutually exclusive.** A Trouve with both raises an error.

## Field reference

These are the `Trouve` fields that apply to pandas execution. See the [Trouve API reference](../reference/trouve-api.md) for the full list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `df_fn` | `Callable \| None` | `None` | Python function that returns the output DataFrame. Its parameter defaults declare the upstream Trouves. Mutually exclusive with `sql`. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional — clair uses them for the documentation. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. They run after clair writes the output. |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |

## Complete example

`example_projects/example_4/` in the repository is a runnable project that uses `df_fn`. See `example_4_database/derived/daily_event_counts.py`.
