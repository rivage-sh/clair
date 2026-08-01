# Trouve API

```python
from clair import Trouve, TrouveType
```

## `TrouveType`

```python
class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE  = "table"
    VIEW   = "view"
```

## `ExecutionType`

```python
class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS    = "pandas"
```

## `Trouve`

```python
class Trouve(BaseModel):
    type:       TrouveType = TrouveType.TABLE
    sql:        str = ""
    df_fn:      Callable | None = None
    columns:    list[Column] = []
    tests:      list[AnyTest] = []
    docs:       str = ""
    run_config: RunConfig = RunConfig()
    compiled:   CompiledAttributes | None = None
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `TrouveType` | `TABLE` | Whether this is a SOURCE, TABLE, or VIEW |
| `sql` | `str` | `""` | SQL query. Required for TABLE/VIEW. Must be empty for SOURCE. Use f-strings to reference other Trouves. Mutually exclusive with `df_fn`. |
| `df_fn` | `Callable \| None` | `None` | Pandas execution mode (alternative to `sql`). TABLE-only, full-refresh-only. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional for TABLE/VIEW. Required for UPSERT. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](tests-api.md). |
| `docs` | `str` | `""` | Documentation string shown in `clair docs`. |
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [RunConfig](run-config-api.md). |
| `compiled` | `CompiledAttributes \| None` | `None` | Set by discovery. Do not set manually. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_compiled` | `bool` | `True` once the project has been discovered |
| `full_name` | `str` | Fully-qualified Snowflake name (`database.schema.table`). Raises `RuntimeError` if not compiled. |

### Validation rules

- TABLE and VIEW: `sql` must be non-empty
- SOURCE: `sql` must be empty
- INCREMENTAL mode: only TABLE supports it (not VIEW)
- `df_fn`: TABLE-only; full-refresh-only; mutually exclusive with `sql`

## `CompiledAttributes`

Set by discovery on each `Trouve.compiled`. Available after `clair compile` or `clair run`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `full_name` | `str` | Routed Snowflake name (used in SQL and DDL) |
| `logical_name` | `str` | Filesystem-derived name (used in DAG edges and selectors) |
| `resolved_sql` | `str` | SQL with all placeholder tokens replaced by real full_names |
| `file_path` | `Path` | Absolute path to the Trouve file |
| `imports` | `list[str]` | Logical names of upstream Trouves |
| `execution_type` | `ExecutionType` | SNOWFLAKE or PANDAS |

## Pandas execution (`df_fn`)

A Trouve runs in pandas when you set `df_fn` in place of `sql`. There is no separate class.

```python
from clair import Trouve
```

### Behaviour

| Aspect | Detail |
|--------|--------|
| Dependencies | Each parameter of `df_fn` whose default value is a `Trouve` becomes an upstream dependency. Clair passes the fetched DataFrame as that keyword argument. |
| Materialization | Always `TABLE`. Clair creates or replaces the table. |
| Incremental | Not available. Full-refresh only. |
| Return value | The function must return a `pd.DataFrame`. Any other type fails the run. |
| Installation | No extra needed. pandas is a dependency of clair. |

### Constraints

- `sql` and `df_fn` are mutually exclusive. A Trouve with both raises `ValueError`.
- `df_fn` must be callable.
- A `df_fn` Trouve must be `TrouveType.TABLE`. A VIEW or SOURCE raises `ValueError`.
- A `df_fn` Trouve does not support incremental run modes.

### Example

```python
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import Column, ColumnType, TestNotNull, Trouve


def top_rated(
    catalog: pd.DataFrame = catalog_trouve,  # type: ignore
    reviews: pd.DataFrame = reviews_trouve,  # type: ignore
) -> pd.DataFrame:
    df = catalog.merge(reviews, on="product_id")
    return (
        df.groupby(["product_id", "name"], as_index=False)["rating"]
        .mean()
        .query("rating >= 4")
    )


trouve = Trouve(
    df_fn=top_rated,
    columns=[
        Column(name="product_id", type=ColumnType.STRING),
        Column(name="name",       type=ColumnType.STRING),
        Column(name="rating",     type=ColumnType.FLOAT),
    ],
    tests=[TestNotNull(column="product_id")],
)
```

See the [Pandas-native guide](../guides/pandas-native.md) for a full walkthrough.

## The f-string pattern

When you reference a Trouve in an f-string:

```python
sql=f"SELECT * FROM {other_trouve}"
```

Python calls `Trouve.__format__`, which:

1. Registers `other_trouve` in a global registry
2. Returns a placeholder token like `__CLAIR_TROUVE_140234567890__`

During discovery, clair replaces every placeholder with the real `full_name` of the referenced Trouve. This is how the dependency graph is built and how SQL names are resolved.
