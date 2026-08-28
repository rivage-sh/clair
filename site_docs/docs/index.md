# clair

Python-native data transformations for Snowflake.

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

Import the upstream Trouve and use it in the f-string. clair does the rest.

## Why clair

- **Dependencies are Python imports.** clair reads the lineage from the import graph, not from a separate metadata layer.
- **Compile first, run second.** `clair compile` resolves the full DAG and writes SQL to `_clairtifacts/` before it connects to Snowflake.
- **clair includes incremental strategies.** Use APPEND and UPSERT modes with no boilerplate. Attach a [`RunConfig`](reference/run-config-api.md) to any [`Trouve`](topics/trouve.md).
- **Data quality as code.** Tests are Pydantic objects on the Trouve itself, not a separate test file.
- **Pandas-native transformations.** Use a [`PandasTrouve`](topics/pandas-native.md) to write any step as a Python function. clair reads the upstream tables as DataFrames, runs your code on your machine, then writes the result to Snowflake.
- **Seeds in Python.** Use a [`SeedTrouve`](topics/seeds.md) for a small table that a person maintains by hand. The rows live in the Python file, and clair builds the table in the same run as every other Trouve.

## Install

```bash
uv tool install rivage-clair
```

Show the version:

```bash
clair --version
```

## Quick links

- :material-rocket-launch: [Quickstart](quickstart.md) — from zero to first run
- :material-console: [CLI reference](cli/overview.md) — all commands and flags
- :material-book-open-variant: [Topics](topics/index.md) — Trouve, DAG, environments, and each configuration subject
- :material-code-braces: [Reference](reference/trouve-api.md) — Python API
