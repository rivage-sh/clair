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
| `type` | `TrouveType` | `TABLE` | Tells you if this is a SOURCE, a TABLE, or a VIEW |
| `sql` | `str` | `""` | SQL query. Required for TABLE/VIEW. Must be empty for SOURCE. Use f-strings to reference other Trouves. Mutually exclusive with `df_fn`. |
| `df_fn` | `Callable \| None` | `None` | Pandas execution mode (alternative to `sql`). TABLE-only, full-refresh-only. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional for TABLE/VIEW. Required for UPSERT. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](tests-api.md). |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [RunConfig](run-config-api.md). |
| `compiled` | `CompiledAttributes \| None` | `None` | Discovery sets this. Do not set it manually. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_compiled` | `bool` | `True` after clair discovers the project |
| `full_name` | `str` | Fully-qualified Snowflake name (`database.schema.table`). Raises `RuntimeError` if not compiled. |

### Validation rules

- TABLE and VIEW: `sql` must be non-empty
- SOURCE: `sql` must be empty
- INCREMENTAL mode: only TABLE supports it (not VIEW)
- `df_fn`: TABLE-only; full-refresh-only; mutually exclusive with `sql`

## `CompiledAttributes`

Discovery sets these attributes on `Trouve.compiled`. They are available after `clair compile` or `clair run`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `full_name` | `str` | The routed Snowflake name. clair uses it in the SQL and the DDL. |
| `logical_name` | `str` | The name from the file system. clair uses it for the DAG edges and the selectors. |
| `resolved_sql` | `str` | The SQL. clair replaced each placeholder token with a real full_name. |
| `file_path` | `Path` | Absolute path to the Trouve file |
| `imports` | `list[str]` | The logical names of the upstream Trouves |
| `execution_type` | `ExecutionType` | SNOWFLAKE or PANDAS |

## Pandas execution (`df_fn`)

A Trouve runs in pandas when you set `df_fn` in place of `sql`. There is no separate class.

```python
from clair import Trouve
```

### Behaviour

| Aspect | Detail |
|--------|--------|
| Dependencies | Each parameter of `df_fn` with a `Trouve` as its default value becomes an upstream dependency. clair passes the DataFrame as that keyword argument. |
| Materialization | Always `TABLE`. clair creates or replaces the table. |
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

See the [Pandas-native guide](../guides/pandas-native.md) for a full example.

## The f-string pattern

When you reference a Trouve in an f-string:

```python
sql=f"SELECT * FROM {other_trouve}"
```

Python calls `Trouve.__format__`, which:

1. Registers `other_trouve` in a global registry
2. Returns a placeholder token, such as `__CLAIR_TROUVE_140234567890__`

At discovery, clair replaces every placeholder with the real `full_name` of the Trouve. Thus clair builds the dependency graph and resolves the SQL names.
