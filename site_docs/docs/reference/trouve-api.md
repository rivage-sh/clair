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

## `PandasTrouve`

```python
from clair import PandasTrouve
```

```python
class PandasTrouve(BaseModel):
    inputs:    dict[str, Trouve]
    transform: Callable[[dict[str, pd.DataFrame]], pd.DataFrame]
    columns:   list[Column] = []
    tests:     list[AnyTest] = []
    docs:      str = ""
    compiled:  CompiledAttributes | None = None
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inputs` | `dict[str, Trouve]` | required | Maps string keys to upstream Trouves. Keys become the keys passed to `transform` at runtime. |
| `transform` | `Callable` | required | Function that receives `dict[str, pd.DataFrame]` and returns a `pd.DataFrame`. Runs locally on the clair machine. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](tests-api.md). |
| `docs` | `str` | `""` | Documentation string shown in `clair docs`. |
| `compiled` | `CompiledAttributes \| None` | `None` | Set by discovery. Do not set manually. |

### Constraints

- Always materializes as `TABLE` (CREATE OR REPLACE).
- Always full-refresh — `run_config` is not supported.
- `transform` must return a `pd.DataFrame`; returning anything else raises an error at run time.
- Requires `clair[pandas]` to be installed; raises `ImportError` at construction time otherwise.

### Example

```python
import pandas as pd
from clair import PandasTrouve, Column, ColumnType, TestNotNull
from refined.products.catalog import trouve as catalog
from refined.products.reviews import trouve as reviews

def top_rated(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = inputs["catalog"].merge(inputs["reviews"], on="product_id")
    return (
        df.groupby(["product_id", "name"])["rating"]
        .mean()
        .reset_index()
        .query("rating >= 4")
    )

trouve = PandasTrouve(
    inputs={"catalog": catalog, "reviews": reviews},
    transform=top_rated,
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
