# Trouve API

```python
from clair import BuildPlan, PandasTable, SeedTable, Source, SqlTrouve, Table, TrouveAbc, TrouveType, View
```

Clair has one class for each kind of object. The class gives the type, thus no
Trouve takes a `type` argument. `TrouveAbc` is the abstract base that each class
shares.

| Class | Type | Backend | Declares dependencies with |
|-------|------|---------|----------------------------|
| `TrouveAbc` | — | none — abstract base | — |
| `Source` | SOURCE | none — clair reads it, and never writes it | — |
| `SqlTrouve` | — | none — abstract base of `Table` and `View` | — |
| `Table` | TABLE | Snowflake SQL | f-string references in `sql` |
| `View` | VIEW | Snowflake SQL | f-string references in `sql` |
| `DataframeTrouve` | TABLE | none — abstract base | — |
| `PandasTable` | TABLE | pandas | the `inputs` list |
| `SeedTable` | TABLE | pandas | none — a seed reads no Trouve |

Each class rejects a field that it does not read. For example, `View(run_config=...)`
raises `ValueError`, because a view has no incremental mode.

## `TrouveType`

```python
class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE  = "table"
    VIEW   = "view"
```

Read it from `trouve.type`. Routing gets it as the `trouve_type` argument.

## `ExecutionType`

```python
class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS    = "pandas"
```

## `TrouveAbc`

The abstract base of each Trouve. It holds the fields that each class shares.
You do not instantiate it directly.

```python
class TrouveAbc(SessionConfig, ABC):
    columns:  list[Column] = []
    tests:    list[AnyTest] = []
    docs:     str = ""
    compiled: CompiledAttributes | None = None

    @property
    def type(self) -> TrouveType: ...                   # the class gives it

    @property
    def execution_type(self) -> ExecutionType: ...      # each subclass supplies it

    def upstream_trouves(self) -> list[TrouveAbc]: ...  # each subclass supplies it
```

Write `isinstance(obj, TrouveAbc)` to accept a Trouve of any class.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `columns` | `list[Column]` | `[]` | Column definitions. Required for UPSERT. |
| `tests` | `list[AnyTest]` | `[]` | Data quality tests. See [Tests](tests-api.md). |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |
| `compiled` | `CompiledAttributes \| None` | `None` | Discovery sets this. Do not set it manually. |
| `warehouse`, `role`, `query_tags`, `statement_timeout_seconds` | | `None`, `None`, `{}`, `None` | The session of this Trouve. A value here replaces the value of the schema and the database. See [Per-Database & Schema Config](../topics/per-database-schema-config.md). |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `type` | `TrouveType` | SOURCE, TABLE, or VIEW. The class gives it. |
| `is_compiled` | `bool` | `True` after clair discovers the project |
| `physical_address` | `TrouveAddress` | The address that clair writes to. `str()` gives `database.schema.table`. Raises `RuntimeError` if not compiled. |

## `Source`

A table that another tool loads. Clair reads it, and never writes it. It takes
only the fields of `TrouveAbc`.

```python
from clair import Source

trouve = Source(docs="The raw orders. The ingestion job loads them.")
```

## `SqlTrouve`

The abstract base of `Table` and `View`. Snowflake materializes each one from SQL.

```python
class SqlTrouve(TrouveAbc, ABC):
    sql: str

    def build_sql(self, effective_mode, run_id, staging_address=None, target_column_names=None) -> BuildPlan: ...
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sql` | `str` | required | The SQL query. It must not be empty. Use f-strings to reference other Trouves. |

| Method | Returns | Description |
|--------|---------|-------------|
| `build_sql(effective_mode, run_id, staging_address=None, target_column_names=None)` | `BuildPlan` | The statements that write the object. Raises `RuntimeError` if not compiled. A subclass can override it. See [Write your own build](../guides/custom-build-sql.md). |

Write `isinstance(obj, SqlTrouve)` to accept a `Table` or a `View`.

## `Table`

A table that Snowflake makes from the SQL.

```python
class Table(SqlTrouve):
    run_config: RunConfig = FullRefresh()
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [RunConfig](run-config-api.md). |

| Method | Returns | Description |
|--------|---------|-------------|
| `insert_statement(address, new_rows, target_column_names)` | `str` | An `INSERT` that names each column of the target. `new_rows` is a table name, or a subquery in parentheses. With no `target_column_names`, it names the declared `columns`. Use it in a `build_sql()` override. |

## `View`

A view that Snowflake makes from the SQL. Each run makes it again. A `View` has
no `run_config`, because a view has no incremental mode.

## `BuildPlan`

The statements that `SqlTrouve.build_sql()` gives for one Trouve.

```python
class BuildPlan(BaseModel):
    statements:         list[str] = []
    cleanup_statements: list[str] = []

    @property
    def all_statements(self) -> list[str]: ...  # statements, then cleanup_statements
```

| Field | Type | Description |
|-------|------|-------------|
| `statements` | `list[str]` | The statements that write the object, in order. clair stops at the first one that fails. |
| `cleanup_statements` | `list[str]` | The statements that remove each object that the plan makes for one run. clair executes them after `statements`, when they pass and when one of them fails. A failed cleanup statement does not fail the Trouve. |
| `all_statements` | `list[str]` | Property. Each statement in the order of a run that passes. |

UPSERT puts the drop of its merge source in `cleanup_statements`.

## `CompiledAttributes`

Discovery sets these attributes on `trouve.compiled`. They are available after `clair compile` or `clair run`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `physical_address` | `TrouveAddress` | The address that routing gives. clair uses it in the SQL and the DDL. |
| `logical_address` | `TrouveAddress` | The address that the file system gives. clair uses it for the DAG edges and the selectors. |
| `resolved_sql` | `str` | The SQL. clair replaced each placeholder token with a real logical address. It is empty for a `PandasTable`. |
| `resolved_transform` | `str` | The source text of the transform function. It is empty for a `Table` or a `View`. |
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

Write `isinstance(obj, DataframeTrouve)` to accept a `PandasTable` or a
`SeedTable`.

## `PandasTable`

The pandas backend. A Python function materializes it.

```python
class PandasTable(DataframeTrouve):
    transform: Callable[..., pd.DataFrame]
    inputs:    list[TrouveAbc] = []
```

```python
from clair import PandasTable
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `transform` | `Callable[..., pd.DataFrame]` | required | The function that gives the output DataFrame. |
| `inputs` | `list[TrouveAbc]` | `[]` | The upstream Trouves. Clair binds them to the transform parameters by position. |

`TrouveAbc` holds the other fields: `columns`, `tests`, `docs`.

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
- A `PandasTable` is always a TABLE, and it has no `run_config`. Each run replaces the table.

### Example

```python
import pandas as pd
from refined.products.catalog import trouve as catalog_trouve
from refined.products.reviews import trouve as reviews_trouve

from clair import Column, ColumnType, PandasTable, TestNotNull


def top_rated(catalog: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    merged = catalog.merge(reviews, on="product_id")
    return (
        merged.groupby(["product_id", "name"], as_index=False)
        .agg(rating=("rating", "mean"))
        .query("rating >= 4")
    )


trouve = PandasTable(
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

## `SeedTable`

A table that holds its rows in the Python file.

```python
class SeedTable(DataframeTrouve):
    dataframe: pd.DataFrame
```

```python
from clair import SeedTable
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dataframe` | `pd.DataFrame` | required | The rows that clair writes. Clair reads it when it imports the file. |

`TrouveAbc` holds the other fields: `columns`, `tests`, `docs`.

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

- A `SeedTable` is always a TABLE, and it has no `run_config`. Each run replaces the table.
- Each column name of the DataFrame must be a string, and the names must be unique.
- The DataFrame needs one column minimum. A seed with no row is valid.

### Example

```python
import pandas as pd

from clair import Column, ColumnType, SeedTable

frame = pd.DataFrame(
    {"country_code": ["US", "FR"], "tax_rate": [0.0, 0.20]}
)
frame["country_code"] = frame["country_code"].astype("string")

trouve = SeedTable(
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

Python calls `TrouveAbc.__format__`, which:

1. Registers `other_trouve` in a global registry
2. Returns a placeholder token, such as `__CLAIR_TROUVE_140234567890__`

At discovery, clair replaces every placeholder with the logical address of the Trouve. Thus clair builds the dependency graph and resolves the SQL names.
