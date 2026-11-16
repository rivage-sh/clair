# clair

Python-native data transformations for Snowflake.

```python
# derived/products/top_reviewed.py  →  derived.products.top_reviewed
from clair import Table
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Table(
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

Each table or view is one Python file, a [Trouve](topics/trouve.md). The path gives the
Snowflake name. Import the upstream Trouve, and put it in the f-string: clair reads the
dependency from that reference.

## Why clair

- **No template language.** The SQL is a Python f-string, and shared logic is a Python
  function. Your editor completes each name and finds each reference.
- **No configuration language.** The directory tree gives the names and the defaults. YAML
  holds the credentials only.
- **Compile first, run second.** `clair compile` writes each statement of the run to
  `_clairtifacts/` with no Snowflake connection. A pull request shows the complete plan.
- **Production gets tested data only.** clair builds each Trouve at a
  [staging address](topics/staging.md), runs its tests there, and promotes it only when
  the tests pass.
- **Incremental strategies.** `Append`, `Upsert` and `DeleteInsert` on a
  [`Table`](reference/run-config-api.md). clair compares the columns before each write.
- **Tests on the Trouve.** A [test](topics/data-quality-tests.md) is a Python object in
  the `tests` list of the Trouve, not a separate file.
- **pandas and seeds.** A [`PandasTable`](topics/pandas-native.md) runs a Python function
  on DataFrames. A [`SeedTable`](topics/seeds.md) holds its rows in the Python file.

## Install

```bash
uv tool install rivage-clair
clair --version
```

See [Installation](installation.md) to add clair to your project.

## Where to go next

- :material-rocket-launch: [Quickstart](quickstart.md) — from an empty directory to a first run
- :material-book-open-variant: [Topics](topics/index.md) — what each part of clair does, and why
- :material-map-marker-path: [Guides](guides/control-the-lifecycle.md) — recipes for one goal each
- :material-console: [CLI reference](cli/overview.md) — each command and each flag
- :material-code-braces: [Python API reference](reference/index.md) — each class and each function
