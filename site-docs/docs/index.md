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

Import the upstream, use it in the f-string — clair figures out the rest.

## Why clair

- **Dependencies are Python imports.** Lineage is derivable from the import graph, not from a separate metadata layer.
- **Compile first, run second.** `clair compile` resolves the full DAG and writes SQL to `_clairtifacts/` before touching Snowflake.
- **Incremental strategies built in.** APPEND and UPSERT modes with no boilerplate — attach a [`RunConfig`](reference/run-config-api.md) to any [`Trouve`](concepts/trouve.md).
- **Data quality as code.** Tests are Pydantic objects on the Trouve itself, not a separate test file.
- **Pandas-native transformations.** Use [`PandasTrouve`](guides/pandas-native.md) to write any step as a Python function — clair fetches upstream tables as DataFrames, runs your code locally, and writes the result back to Snowflake.

## Install

```bash
uv tool install git+https://github.com/OmerBaddour/clair.git
```

Verify:

```bash
clair --version
```

## Quick links

- :material-rocket-launch: [Quickstart](quickstart.md) — from zero to first run
- :material-console: [CLI reference](cli/overview.md) — all commands and flags
- :material-book-open-variant: [Concepts](concepts/trouve.md) — Trouve, DAG, environments
- :material-code-braces: [Reference](reference/trouve-api.md) — Python API
