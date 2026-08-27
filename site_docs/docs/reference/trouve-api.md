# Trouve API

```python
from clair import PandasTrouve, SeedTrouve, Trouve, TrouveAbc, TrouveType
```

Clair has one Trouve class for each backend. `TrouveAbc` is the abstract base
that they share. `Trouve` runs SQL in Snowflake. `PandasTrouve` and `SeedTrouve`
both give a DataFrame, which clair writes to Snowflake.

| Class | Backend | Declares dependencies with |
|-------|---------|----------------------------|
| `TrouveAbc` | none — abstract base | — |
| `Trouve` | Snowflake SQL | f-string references in `sql` |
| `DataframeTrouve` | none — abstract base | — |
| `PandasTrouve` | pandas | the `inputs` list |
| `SeedTrouve` | pandas | none — a seed reads no Trouve |

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

## `DataframeTrouve`

The abstract base of each backend that clair writes from a DataFrame. Clair
reads each upstream Trouve into a DataFrame, calls `build_dataframe`, and writes
the result. Its `execution_type` is always `PANDAS`.

```python
class DataframeTrouve(TrouveAbc, ABC):
    def build_dataframe(self, *input_dataframes: pd.DataFrame) -> pd.DataFrame: ...

    def parameter_names(self) -> list[str]: ...  # a name for each input
    def source_text(self) -> str: ...            # what `clair compile` writes
    def source_file(self) -> str | None: ...     # the file of the import section
```

Write `isinstance(obj, DataframeTrouve)` to accept a `PandasTrouve` or a
`SeedTrouve`.

## `PandasTrouve`

The pandas backend. A Python function materializes it.

```python
class PandasTrouve(DataframeTrouve):
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

See the [Pandas-Native Transformations](../topics/pandas-native.md) for a full example.

## `SeedTrouve`

A table that holds its rows in the Python file.

```python
class SeedTrouve(DataframeTrouve):
    dataframe: pd.DataFrame
```

```python
from clair import SeedTrouve
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dataframe` | `pd.DataFrame` | required | The rows that clair writes. Clair reads it when it imports the file. |

`TrouveAbc` holds the other fields: `columns`, `tests`, `docs`, `run_config`.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `upstream_trouves()` | `list[TrouveAbc]` | Always empty. A seed reads no other Trouve. |
| `build_dataframe()` | `pd.DataFrame` | The `dataframe` field. |
| `source_text()` | `str` | The dtypes and the rows, for the compile artifact. |

### Behaviour

| Aspect | Detail |
|--------|--------|
| Dependencies | None. A seed is always a root of the DAG. |
| Materialization | Always `TABLE`. clair creates or replaces the table each run. |
| Incremental | Not available. Full-refresh only. |
| Column types | The dtype of each column gives the Snowflake type. `columns` stays documentation. |
| Command | None. `clair run` builds a seed with every other Trouve. |

### Constraints

- A `SeedTrouve` must be `TrouveType.TABLE`. A VIEW or a SOURCE raises `ValueError`.
- A `SeedTrouve` does not support incremental run modes.
- Each column name of the DataFrame must be a string, and the names must be unique.
- The DataFrame needs one column minimum. A seed with no row is valid.

### Example

```python
import pandas as pd

from clair import Column, ColumnType, SeedTrouve

frame = pd.DataFrame(
    {"country_code": ["US", "FR"], "tax_rate": [0.0, 0.20]}
)
frame["country_code"] = frame["country_code"].astype("string")

trouve = SeedTrouve(
    dataframe=frame,
    docs="The tax rate of each country.",
    columns=[
        Column(name="country_code", type=ColumnType.STRING),
        Column(name="tax_rate",     type=ColumnType.FLOAT),
    ],
)
```

See the [Seeds](../topics/seeds.md) page for a full example.

## The f-string pattern

When you reference a Trouve in an f-string:

```python
sql=f"SELECT * FROM {other_trouve}"
```

Python calls `Trouve.__format__`, which:

1. Registers `other_trouve` in a global registry
2. Returns a placeholder token, such as `__CLAIR_TROUVE_140234567890__`

At discovery, clair replaces every placeholder with the logical address of the Trouve. Thus clair builds the dependency graph and resolves the SQL names.
