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
- **clair includes incremental strategies.** Use APPEND and UPSERT modes with no boilerplate. Attach a [`RunConfig`](reference/run-config-api.md) to any [`Trouve`](concepts/trouve.md).
- **Data quality as code.** Tests are Pydantic objects on the Trouve itself, not a separate test file.
- **Pandas-native transformations.** Use a [`PandasTrouve`](guides/pandas-native.md) to write any step as a Python function. clair reads the upstream tables as DataFrames, runs your code on your machine, then writes the result to Snowflake.

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
- :material-book-open-variant: [Concepts](concepts/trouve.md) — Trouve, DAG, environments
- :material-code-braces: [Reference](reference/trouve-api.md) — Python API
