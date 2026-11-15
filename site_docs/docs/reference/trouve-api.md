# Trouve API

```python
from clair import MaterializedTrouve, NodePlan, PandasTable, PlanContext, SeedTable, Source, SqlTrouve, Table, TestQuery, TrouveAbc, TrouveType, View
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
class TrouveAbc(SessionConfig, ABC, Generic[CompiledT_co]):
    columns:  list[Column] = []
    tests:    list[AnyTest] = []
    docs:       str = ""
    sample_sql: str | None = None
    compiled:   CompiledT_co | None = None    # CompiledAttributes, or a subclass of it

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
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it, and clair writes it as the comment of the object in Snowflake. See [Comments in Snowflake](../topics/staging.md#comments-in-snowflake). |
| `sample_sql` | `str \| None` | `None` | The query that a sample reads in place of this Trouve. Write `{THIS}` for the address of this Trouve. It can read another Trouve, which then is an upstream Trouve. `None` gives a row limit. See [Sampled Runs](../topics/sampling.md). |
| `compiled` | `CompiledAttributes \| None` | `None` | Discovery sets this. Do not set it manually. The class of the Trouve decides the class of the value. See [`CompiledAttributes`](#compiledattributes). |
| `warehouse`, `role`, `query_tags`, `statement_timeout_seconds` | | `None`, `None`, `{}`, `None` | The session of this Trouve. A value here replaces the value of the schema and the database. See [Per-Database & Schema Config](../topics/per-database-schema-config.md). |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `type` | `TrouveType` | SOURCE, TABLE, or VIEW. The class gives it. |
| `logical_address` | `TrouveAddress` | The address that the file path gives, and the production address. The file alone decides it, thus a Trouve that you import outside discovery has it too. Raises `TrouveFileNotFoundError` if no file, or two files, define the Trouve. |
| `is_compiled` | `bool` | `True` after clair discovers the project |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `get_physical_address(env_name)` | `TrouveAddress` | The address that `clair run --env <env_name>` writes to. clair finds `__routing__.py` above the file of the Trouve, and it routes the logical address. It needs no discovery and no connection. Raises `TrouveFileNotFoundError` or `ProjectRootNotFoundError`. |
| `require_compiled()` | `CompiledAttributes` | The record of the run that loaded this Trouve: `trouve.compiled`. Raises `TrouveNotCompiledError` if discovery did not load it. |
| `sample(sample_rows)` | `str` | The subquery that `--sample` reads in place of this Trouve: `({sample_sql})`, or `(SELECT * FROM {THIS} LIMIT <sample_rows>)`. clair replaces `{THIS}` with the address that the reader reads. Override it in a subclass to change the sample of many Trouves. |

## `Source`

A table that another tool loads. Clair reads it, and never writes it. It takes
only the fields of `TrouveAbc`.

```python
from clair import Source

trouve = Source(docs="The raw orders. The ingestion job loads them.")
```

## `MaterializedTrouve`

The abstract base of each Trouve that clair writes: `Table`, `View`, `PandasTable` and
`SeedTable`. It gives the lifecycle of the Trouve: `plan()` and one method for each phase.
[Control the lifecycle of a Trouve](../guides/control-the-lifecycle.md) tells you what
each phase does, when clair executes it, and how to override it.

| Method | Returns | Description |
|--------|---------|-------------|
| `effective_mode(requested_mode, current_object)` | `RunMode` | The policy that gives the mode of one node. The default gives `FULL_REFRESH`. A `Table` can give `INCREMENTAL`. |
| `should_promote(context, test_results)` | `bool` | The policy that decides the promotion of a staged run. |
| `plan(context)` | `NodePlan` | Each statement of the node, by phase. |
| `prepare(context)`, `build(context)`, `configure(context)`, `test(context)`, `promote(context)`, `document(context)`, `retire(context)`, `cleanup(context)` | `list[str]`, or `list[TestQuery]` for `test` | The statements of one phase. |
| `comment_statements(address, current_comments)`, `drop_statement(address)` | `list[str]`, `str` | Building blocks for an override. |
| `warehouse_object_type` | `WarehouseObjectType` | Property. `TABLE`, `ICEBERG_TABLE` or `VIEW`. |

## `SqlTrouve`

The abstract base of `Table` and `View`. Snowflake materializes each one from SQL.

```python
class SqlTrouve(MaterializedTrouve, ABC):
    sql: str

    def build(self, context: PlanContext) -> list[str]: ...
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sql` | `str` | required | The SQL query. It must not be empty. Use f-strings to reference other Trouves. |

| Method | Returns | Description |
|--------|---------|-------------|
| `create_statement(address)` | `str` | The `CREATE OR REPLACE` of a full refresh. |
| `resolved_sql()` | `str` | The SQL with each reference resolved. Raises `TrouveNotCompiledError` if not compiled. |
| `sql_for(run_mode)` | `str` | The SQL as the module writes it when `clair.run_mode` is `run_mode`. clair runs the file again if the mode differs from the mode of the command line. A build gives `sql_for(context.effective_mode)`. |
| `full_refresh_sql()` | `str` | `sql_for(RunMode.FULL_REFRESH)`: the SQL without an incremental filter. |

Write `isinstance(obj, SqlTrouve)` to accept a `Table` or a `View`.

## `Table`

A table that Snowflake makes from the SQL.

```python
class Table(SqlTrouve):
    run_config: RunConfig = FullRefresh()
    transient: bool | None = None
    cluster_by: list[str] = []
    iceberg: Iceberg | None = None
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_config` | `RunConfig` | full refresh | Materialization strategy. See [RunConfig](run-config-api.md). |
| `transient` | `bool \| None` | `None` | `True` makes a TRANSIENT table: no Fail-safe, and one day of Time Travel maximum. `False` makes a permanent table. `None` takes the value of `__schema_config__.py`, then of `__database_config__.py`, and then `False`. See [A transient table](../topics/staging.md#a-transient-table). |
| `cluster_by` | `list[str]` | `[]` | The clustering key: a column name or an SQL expression for each part. Snowflake reclusters the table in the background. See [A clustered table](../topics/staging.md#a-clustered-table). |
| `iceberg` | `Iceberg \| None` | `None` | `Iceberg(...)` makes a Snowflake-managed Iceberg table. A `transient` Iceberg table is rejected. See [An Iceberg table](../topics/staging.md#an-iceberg-table). |

`Table`, `PandasTable` and `SeedTable` have the property `is_transient`: `True` if a run makes a TRANSIENT table. Discovery resolves it from `transient` and the directory configs. It raises `TrouveNotCompiledError` before discovery. An Iceberg table is never transient.

| Method | Returns | Description |
|--------|---------|-------------|
| `insert_statement(address, new_rows, target_column_names)` | `str` | An `INSERT` that names each column of the target. `new_rows` is a table name, or a subquery in parentheses. With no `target_column_names`, it names the declared `columns`. |
| `merge_statement(target_address, source_address)` | `str` | The `MERGE` of the `Upsert` strategy. |
| `delete_replaced_rows_statement(target_address, source_address)` | `str` | The `DELETE` of the `DeleteInsert` strategy. |
| `create_new_rows_statement(run_id)` | `str` | The table of the new rows of this run, at `new_rows_address(run_id)`. |
| `new_rows_address(run_id)` | `str` | The address of the new rows table of an `Upsert` or a `DeleteInsert`. |
| `clone_statement(source_address, clone_address)` | `str` | A zero-copy clone. |
| `add_column_statements(address, added_columns)` | `list[str]` | One `ALTER TABLE ... ADD COLUMN` for each column. |
| `cluster_by_statements(address, incremental=...)` | `list[str]` | The `ALTER` that sets or drops the clustering key. |
| `clone_promote_statements(staging_address, physical_address)` | `list[str]` | The default promotion: a clone with `COPY GRANTS`. |

Each method gives SQL text, and sends nothing. Use them in an override of a phase. See
[The building blocks](../guides/control-the-lifecycle.md#the-building-blocks).

## `Iceberg`

The settings of a Snowflake-managed Apache Iceberg table. Snowflake is the catalog.

```python
from clair import Iceberg

class Iceberg(BaseModel):
    external_volume: str | None = None
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `external_volume` | `str \| None` | `None` | The external volume that holds the data files and the metadata files. `None` takes the default of the schema, the database or the account, then `SNOWFLAKE_MANAGED`. |

## `View`

A view that Snowflake makes from the SQL. Each run makes it again. A `View` has
no `run_config`, because a view has no incremental mode.

## `NodePlan`

The statements that `plan()` gives for one node, by phase. The runner executes the phases
in the order of the fields.

```python
class NodePlan(BaseModel):
    prepare:   list[str] = []
    build:     list[str] = []
    configure: list[str] = []
    test:      list[TestQuery] = []
    promote:   list[str] = []
    document:  list[str] = []
    retire:    list[str] = []
    cleanup:   list[str] = []
```

## `PlanContext`

The facts of one node that `plan()` reads. The runner reads them from the warehouse before
it calls `plan()`. `clair compile` and `clair validate` give an offline context. See
[What a phase reads](../guides/control-the-lifecycle.md#what-a-phase-reads-plancontext).

## `TestQuery`

One data quality test as SQL. Each row of the result is a failure.

```python
class TestQuery(BaseModel):
    test_index: int
    label: str
    column_name: str | None = None
    severity: TestSeverity = TestSeverity.ERROR
    run_with_sample: bool = True
    sql: str
```

`TestQuery.of(test, test_index, address)` gives the query of one `Test` against an address.
The default `test()` phase gives one for each test of the Trouve.

## `CompiledAttributes`

Discovery sets these attributes on `trouve.compiled`. They are available after `clair compile` or `clair run`.
They are the record of one run: the environment, the selection and the run mode decide
them, thus a different run gives different values. What the file alone decides, such as
`trouve.logical_address`, is an attribute of the Trouve itself.

Each Trouve holds these attributes. A `Source` holds these only.

| Attribute | Type | Description |
|-----------|------|-------------|
| `physical_address` | `TrouveAddress` | The address that the routing entry of the run gives. clair writes there. |
| `file_path` | `Path` | The path of the Trouve file, relative to the project root |
| `module_name` | `str` | The module name of the Trouve file |
| `imports` | `list[str]` | The logical addresses of the upstream Trouves |
| `config` | `ResolvedConfig` | The session of the Trouve, after clair applied the schema and the database defaults |

The backend of the Trouve adds its own attributes. Thus `self.compiled` in a `Table`
subclass is a `CompiledSql`, and a type checker knows that it holds `resolved_sql`.

| Class | The Trouve that holds it | Attribute | Type | Description |
|-------|--------------------------|-----------|------|-------------|
| `CompiledSql` | `Table`, `View` | `resolved_sql` | `str` | The SQL. clair replaced each placeholder token with a real address. |
| `CompiledDataframe` | `PandasTable`, `SeedTable` | `resolved_transform` | `str` | The text that shows how the Trouve makes its rows: the source text of the transform function. |
| `CompiledDataframe` | `PandasTable`, `SeedTable` | `inputs` | `list[DataframeInput]` | The read of each input, in the parameter order of the transform: the `address`, and the `sample_query` of a sampled run. |

The Trouve gives its backend: `trouve.execution_type` is `SNOWFLAKE` or `PANDAS`.

## `DataframeTrouve`

The abstract base of each backend that clair writes from a DataFrame. Clair
reads each upstream Trouve into a DataFrame, calls `build_dataframe`, and writes
the result. Its `execution_type` is always `PANDAS`.

```python
class DataframeTrouve(TrouveAbc, ABC):
    transient: bool | None = None
    cluster_by: list[str] = []

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
| `transient` | `bool \| None` | `None` | `True` makes a TRANSIENT table: no Fail-safe, and one day of Time Travel maximum. `False` makes a permanent table. `None` takes the value of `__schema_config__.py`, then of `__database_config__.py`, and then `False`. See [A transient table](../topics/staging.md#a-transient-table). |
| `cluster_by` | `list[str]` | `[]` | The clustering key: a column name or an SQL expression for each part. Snowflake reclusters the table in the background. See [A clustered table](../topics/staging.md#a-clustered-table). |

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
| `transient` | `bool \| None` | `None` | `True` makes a TRANSIENT table: no Fail-safe, and one day of Time Travel maximum. `False` makes a permanent table. `None` takes the value of `__schema_config__.py`, then of `__database_config__.py`, and then `False`. See [A transient table](../topics/staging.md#a-transient-table). |
| `cluster_by` | `list[str]` | `[]` | The clustering key: a column name or an SQL expression for each part. Snowflake reclusters the table in the background. See [A clustered table](../topics/staging.md#a-clustered-table). |

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
