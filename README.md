<p align="center">
  <img src="https://raw.githubusercontent.com/rivage-sh/clair/main/site_docs/docs/assets/clair_logo.png" alt="clair logo" width="200" />
</p>

# clair

Python-native data transformations for Snowflake.

Each table or view is one Python file, a **Trouve**. The path of the file gives the
Snowflake name, and the SQL is a plain f-string. A Trouve reads another Trouve through a
normal Python import:

```python
# derived/products/top_reviewed.py  →  derived.products.top_reviewed
from clair import Table, TestNotNull
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

trouve = Table(
    sql=f"""
        SELECT c.product_id, c.name, avg(r.rating) AS avg_rating
        FROM {catalog} c
        JOIN {reviews} r ON c.product_id = r.product_id
        GROUP BY 1, 2
    """,
    tests=[TestNotNull(column="product_id")],
)
```

clair finds each Trouve, builds the DAG from the references, and runs it in order. It
writes each Trouve to a staging table first, runs the tests there, and replaces
production only when the tests pass.

- **No Jinja, no YAML.** The SQL is Python, thus your editor completes it and finds each
  reference.
- **Compile with no connection.** `clair compile` writes each statement that a run sends,
  so a pull request shows the complete plan.
- **Incremental tables.** `Append`, `Upsert` and `DeleteInsert`, with a check of the
  columns before each write.
- **pandas when SQL is not enough.** A `PandasTable` runs a Python function on DataFrames,
  and a `SeedTable` holds its rows in the file.
- **A Python library first.** `clair.run()`, `clair.compile()` and `clair.test()` give
  data objects. The CLI is a thin caller of them.

## Install

```bash
uv tool install rivage-clair   # the clair command
uv add rivage-clair            # in your project, for the editor and the type checker
```

## First run

```bash
clair init                     # writes ~/.clair/environments.yml and __routing__.py
clair compile --env dev        # shows the SQL, needs no Snowflake connection
clair run --env dev            # builds, tests and promotes each Trouve
```

The [Quickstart](https://rivage-sh.github.io/clair/quickstart/) takes you from an empty
directory to a first run.

## Documentation

**[rivage-sh.github.io/clair](https://rivage-sh.github.io/clair)** holds the topics, the
guides, the CLI reference and the Python API reference.

## Example projects

`examples/projects/` holds projects that you can run:

| Project | What it shows |
|---------|---------------|
| `example_1` | A minimal pipeline of 4 Trouves, with VARIANT flattening |
| `example_2` | A warehouse of 52 Trouves in 5 layers, with a seed in `reference` |
| `example_3` | The incremental strategies APPEND, UPSERT and DELETE_INSERT |
| `example_4` | A `PandasTable` that gives a DataFrame in place of SQL |
| `example_from_init` | The files that `clair init` writes |

Each numbered project has a `dev` entry and a `prod` entry in `__routing__.py`. The `dev`
entry reads `CLAIR_USER`, thus each person writes to a different database. The README of
each project holds the SQL that makes the source tables.

`examples/notebooks/` shows the Python API. The notebooks need no Snowflake account.

## Development

```bash
git clone https://github.com/rivage-sh/clair.git
cd clair
uv sync
uv run pytest -m "not integration"
```
