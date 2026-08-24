# Trouve API

```python
from clair import PandasTrouve, Trouve, TrouveAbc, TrouveType
```

Clair has one Trouve class for each backend. `TrouveAbc` is the abstract base
that they share. `Trouve` runs SQL in Snowflake. `PandasTrouve` runs a Python
function on the machine executing clair.

| Class | Backend | Declares dependencies with |
|-------|---------|----------------------------|
| `TrouveAbc` | none — abstract base | — |
| `Trouve` | Snowflake SQL | f-string references in `sql` |
| `PandasTrouve` | pandas | the `inputs` list |

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

## `TrouveAbc`

The abstract base of every backend. It holds the fields that each backend
shares. You do not instantiate it directly — a subclass supplies the backend.

```python
class TrouveAbc(BaseModel, ABC):
    type:       TrouveType = TrouveType.TABLE
    columns:    list[Column] = []
    tests:      list[AnyTest] = []
    docs:       str = ""
    run_config: RunConfig = RunConfig()
    compiled:   CompiledAttributes | None = None

    @property
    def execution_type(self) -> ExecutionType: ...   # each subclass supplies it

    def upstream_trouves(self) -> list[TrouveAbc]: ...  # each subclass supplies it
```

Write `isinstance(obj, TrouveAbc)` to accept a Trouve of any backend.

## `Trouve`

The SQL backend. Snowflake materializes it.

```python
class Trouve(TrouveAbc):
    sql: str = ""
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `TrouveType` | `TABLE` | Tells you if this is a SOURCE, a TABLE, or a VIEW |
| `sql` | `str` | `""` | SQL query. Required for TABLE/VIEW. Must be empty for SOURCE. Use f-strings to reference other Trouves. |
| `columns` | `list[Column]` | `[]` | Column definitions. Optional for TABLE/VIEW. Required for UPSERT. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](tests-api.md). |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [RunConfig](run-config-api.md). |
| `compiled` | `CompiledAttributes \| None` | `None` | Discovery sets this. Do not set it manually. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_compiled` | `bool` | `True` after clair discovers the project |
| `physical_address` | `TrouveAddress` | The address that clair writes to. `str()` gives `database.schema.table`. Raises `RuntimeError` if not compiled. |

### Validation rules

- TABLE and VIEW: `sql` must be non-empty
- SOURCE: `sql` must be empty
- INCREMENTAL mode: only TABLE supports it (not VIEW)

## `CompiledAttributes`

Discovery sets these attributes on `Trouve.compiled`. They are available after `clair compile` or `clair run`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `physical_address` | `TrouveAddress` | The address that routing gives. clair uses it in the SQL and the DDL. |
| `logical_address` | `TrouveAddress` | The address that the file system gives. clair uses it for the DAG edges and the selectors. |
| `resolved_sql` | `str` | The SQL. clair replaced each placeholder token with a real logical address. It is empty for a `PandasTrouve`. |
| `resolved_transform` | `str` | The source text of the transform function. It is empty for a SQL `Trouve`. |
| `file_path` | `Path` | Absolute path to the Trouve file |
| `imports` | `list[str]` | The logical addresses of the upstream Trouves |
| `execution_type` | `ExecutionType` | SNOWFLAKE or PANDAS |

## `PandasTrouve`

The pandas backend. A Python function materializes it.

```python
class PandasTrouve(TrouveAbc):
    transform: Callable[..., pd.DataFrame]
    inputs:    list[TrouveAbc] = []
```

```python
from clair import PandasTrouve
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `transform` | `Callable[..., pd.DataFrame]` | required | The function that gives the output DataFrame. |
| `inputs` | `list[TrouveAbc]` | `[]` | The upstream Trouves. Clair binds them to the transform parameters by position. |

`TrouveAbc` holds the other fields: `columns`, `tests`, `docs`, `run_config`.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `upstream_trouves()` | `list[TrouveAbc]` | The inputs, in the parameter order of the transform. |
| `parameter_names()` | `list[str]` | The parameter names of the transform, in order. |

### Behaviour

| Aspect | Detail |
|--------|--------|
| Dependencies | Each Trouve in `inputs` becomes an upstream dependency. clair binds each one to a transform parameter by position. |
| Materialization | Always `TABLE`. clair creates or replaces the table. |
| Incremental | Not available. Full-refresh only. |
| Return value | The function must return a `pd.DataFrame`. Any other type fails the run. |
| Installation | No extra needed. pandas is a dependency of clair. |

### Constraints

- The count of `inputs` must equal the count of transform parameters.
- The transform must not use `*args` or `**kwargs`.
- A `PandasTrouve` must be `TrouveType.TABLE`. A VIEW or a SOURCE raises `ValueError`.
- A `PandasTrouve` does not support incremental run modes.

### Example

```python
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import Column, ColumnType, PandasTrouve, TestNotNull


def top_rated(catalog: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    merged = catalog.merge(reviews, on="product_id")
    return (
        merged.groupby(["product_id", "name"], as_index=False)
        .agg(rating=("rating", "mean"))
        .query("rating >= 4")
    )


trouve = PandasTrouve(
    transform=top_rated,
    inputs=[catalog_trouve, reviews_trouve],
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

At discovery, clair replaces every placeholder with the logical address of the Trouve. Thus clair builds the dependency graph and resolves the SQL names.
