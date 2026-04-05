# PandasTrouve -- Architectural Design

**Status:** Proposed
**Date:** 2026-03-26
**Author:** Software Architect Agent

---

## Summary

Add a first-class `PandasTrouve` to Clair: a DAG node whose transformation is a Python function `(dict[str, pd.DataFrame]) -> pd.DataFrame` instead of SQL. Clair owns the I/O (fetch upstream tables as DataFrames, write the result back to Snowflake). The DAG, lineage, selectors, tests, compile, and run commands work identically across SQL and pandas nodes.

---

## 1. PandasTrouve Data Model

### Design decision: separate class, not a Trouve variant

`PandasTrouve` is a **new Pydantic model** that lives alongside `Trouve`, not a subclass or optional-field extension.

**Why not extend Trouve?**

- `Trouve` validates that TABLE/VIEW requires non-empty `sql`. Adding `transform` as an alternative would weaken this validator into a conditional "one of sql or transform" check, making both paths easier to misuse.
- `build_sql()` is core to `Trouve` -- it would need guards everywhere. A separate class with `build_sql()` that always raises `NotImplementedError` (or no `build_sql()` at all) is cleaner.
- The `__format__` placeholder trick is irrelevant for PandasTrouve. Mixing the two contracts in one class creates confusion about which features apply.
- The two types have genuinely different fields: `sql` vs `transform` + `inputs`.

**Why not a shared ABC?**

Too early. If a third node type emerges (e.g. `SparkTrouve`), extracting a `BaseTrouve` protocol is straightforward. For now, two concrete classes that share the same `CompiledAttributes` and `TrouveType` types is sufficient. Discovery and the DAG already operate on duck-typed conventions (module-level `trouve` variable, `.compiled`, `.type`, `.full_name`), so formal inheritance is not needed for polymorphism.

### PandasTrouve fields

```python
from collections.abc import Callable
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from clair.trouves.column import Column
from clair.trouves.run_config import RunConfig
from clair.trouves.test import AnyTest
from clair.trouves.trouve import CompiledAttributes, TrouveType


class PandasTrouve(BaseModel):
    """A DAG node whose transformation is a Python function over DataFrames.

    Attributes:
        inputs: Mapping of argument names to upstream Trouve objects.
            Keys become the dict keys passed to `transform`.
        transform: A callable (dict[str, pd.DataFrame]) -> pd.DataFrame.
        columns: Column definitions for documentation and validation.
        tests: Data quality tests (same as SQL Trouves).
        docs: Documentation string.
        run_config: Materialization config (full_refresh only for v1).
        compiled: Set by discovery. None until the project has been discovered.
    """

    type: TrouveType = Field(default=TrouveType.TABLE, frozen=True)
    inputs: dict[str, Any] = Field(
        ...,
        description="Mapping of argument names to upstream Trouve/PandasTrouve objects",
    )
    transform: Callable[[dict[str, "pd.DataFrame"]], "pd.DataFrame"] = Field(
        ...,
        exclude=True,
        description="The transformation function",
    )
    columns: list[Column] = []
    tests: list[AnyTest] = []
    docs: str = ""
    run_config: RunConfig = Field(default_factory=RunConfig)
    compiled: CompiledAttributes | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def _validate_pandas_trouve(self) -> "PandasTrouve":
        if self.type not in (TrouveType.TABLE,):
            raise ValueError("PandasTrouve only supports TABLE materialization")
        if not self.inputs:
            raise ValueError("PandasTrouve requires at least one input")
        if not callable(self.transform):
            raise ValueError("transform must be callable")
        if self.run_config.run_mode != RunMode.FULL_REFRESH:
            raise ValueError("PandasTrouve does not support incremental mode in v1")
        return self

    @property
    def is_compiled(self) -> bool:
        return self.compiled is not None

    @property
    def full_name(self) -> str:
        if self.compiled is None:
            raise RuntimeError(
                "PandasTrouve.full_name is not set. "
                "This PandasTrouve was not loaded by clair's discovery layer."
            )
        return self.compiled.full_name

    # No __format__ override -- PandasTrouves are not used inside f-string SQL.
    # They CAN be referenced as inputs to other PandasTrouves.
    # They CAN be referenced in SQL Trouves via f-string (register for placeholder).

    def __format__(self, _spec: str) -> str:
        """Allow PandasTrouve to be referenced in f-string SQL by downstream SQL Trouves."""
        from clair.trouves._refs import register
        return register(self)
```

### Key design notes

- `type` is frozen to `TABLE`. Views don't make sense for DataFrame-based transforms (no lazy SQL to wrap). Source also doesn't apply.
- `inputs` is `dict[str, Any]` rather than `dict[str, Trouve]` because an input can be either a `Trouve` or another `PandasTrouve`. Using `Any` avoids a circular import and allows both types. Validation at discovery time confirms all values are recognized Trouves.
- `__format__` is still implemented so that a downstream *SQL* Trouve can do `f"SELECT * FROM {pandas_trouve}"` -- the PandasTrouve materializes to a real table, so its full_name is valid in SQL.
- `transform` is excluded from serialization (`exclude=True`) since callables aren't JSON-serializable.

---

## 2. Dependency Declaration

### User-facing API

```python
# example_projects/example_1/example_1_database/derived/daily_event_summary.py

from clair import PandasTrouve, Column, ColumnType
import pandas as pd

from example_1_database.refined.events import trouve as events
from example_1_database.refined.users import trouve as users


def compute_summary(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    events_df = inputs["events"]
    users_df = inputs["users"]
    merged = events_df.merge(users_df, on="user_id")
    return merged.groupby("event_type").agg(
        total_events=("event_id", "count"),
        unique_users=("user_id", "nunique"),
    ).reset_index()


trouve = PandasTrouve(
    inputs={"events": events, "users": users},
    transform=compute_summary,
    docs="Summary of events by type with unique user counts.",
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="total_events", type=ColumnType.NUMBER),
        Column(name="unique_users", type=ColumnType.NUMBER),
    ],
)
```

### Why `inputs=dict` constructor arg?

| Approach | Pros | Cons |
|---|---|---|
| **`inputs={name: trouve}` dict (chosen)** | Explicit key-name mapping; keys appear in `transform` signature; no magic; discoverable without executing transform | Slightly more verbose than a decorator |
| `@pandas_trouve(inputs=[...])` decorator | Familiar pattern | Hides the Trouve construction; breaks the `trouve = X(...)` convention; harder for discovery to inspect |
| Reuse `{trouve}` in sentinel SQL | Consistent with SQL Trouves | Artificial -- there's no SQL; confusing DX |

### How discovery detects dependencies

The `inputs` dict holds *references to already-imported Trouve/PandasTrouve objects*. Each of these was registered in `_refs._registry` when the importing module's f-string was evaluated (for SQL Trouves), or can be identified by `id()` (since they are the same Python objects loaded during discovery).

Discovery extracts dependencies by:

1. Checking if the module-level `trouve` variable is an instance of `PandasTrouve`.
2. Iterating `trouve.inputs.values()` and mapping each to its `id()` in the existing `id_to_full_name` registry (which is built from all collected Trouves).
3. Returning these as `compiled.imports`.

This is **purely structural inspection** -- the `transform` function is never called. The dependency information is in the `inputs` dict, not in code execution.

---

## 3. Adapter Changes

### Core principle: do not pollute the ABC with pandas

The `WarehouseAdapter` ABC is a warehouse-agnostic contract. Adding `pd.DataFrame` to its signature would:
- Force a pandas dependency on every adapter implementation (including future non-Python warehouses).
- Violate the current design where `base.py` has no heavy dependencies.

### Solution: add `fetch_dataframe` and `write_dataframe` to SnowflakeAdapter only

These are **concrete methods on `SnowflakeAdapter`**, not abstract methods on the ABC. The runner's PandasTrouve execution path knows it needs a pandas-capable adapter and type-checks accordingly.

```python
# In adapters/snowflake.py

import pandas as pd


class WriteResult(BaseModel):
    """Result of writing a DataFrame to Snowflake."""
    success: bool
    row_count: int
    error: str | None = None


class SnowflakeAdapter(WarehouseAdapter):
    # ... existing methods ...

    def fetch_dataframe(
        self,
        full_name: str,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Read a Snowflake table/view into a pandas DataFrame.

        Args:
            full_name: Fully-qualified name (database.schema.table).
            limit: Optional row limit for development/testing.

        Returns:
            A pandas DataFrame with the full table contents.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")
        sql = f"SELECT * FROM {full_name}"
        if limit is not None:
            sql += f" LIMIT {limit}"
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            df = cursor.fetch_pandas_all()
            return df
        finally:
            cursor.close()

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        full_name: str,
        overwrite: bool = True,
    ) -> WriteResult:
        """Write a pandas DataFrame to a Snowflake table.

        Uses write_pandas() from snowflake-connector-python for efficient
        bulk loading (PUT + COPY INTO internally).

        Args:
            dataframe: The DataFrame to write.
            full_name: Fully-qualified target name (database.schema.table).
            overwrite: If True, replace existing data. If False, append.

        Returns:
            WriteResult with success status and row count.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")
        parts = full_name.split(".")
        database_name, schema_name, table_name = parts[0], parts[1], parts[2]

        from snowflake.connector.pandas_tools import write_pandas

        try:
            success, num_chunks, num_rows, _ = write_pandas(
                conn=self._conn,
                df=dataframe,
                table_name=table_name.upper(),
                database=database_name,
                schema=schema_name,
                overwrite=overwrite,
                auto_create_table=True,
                quote_identifiers=False,
            )
            return WriteResult(
                success=success,
                row_count=num_rows,
            )
        except Exception as e:
            return WriteResult(
                success=False,
                row_count=0,
                error=str(e),
            )
```

### Why `auto_create_table=True`?

Consistent with SQL Trouves using `CREATE OR REPLACE TABLE`. The first run creates the table; subsequent runs replace it (when `overwrite=True`).

### Why `fetch_pandas_all()` instead of `pd.read_sql()`?

`cursor.fetch_pandas_all()` is the Snowflake connector's native method. It uses Arrow internally and is significantly faster than `pd.read_sql()` for large result sets. No SQLAlchemy dependency needed.

### Protocol for type-checking

Rather than requiring the ABC to know about pandas, the runner can check for the capability:

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class DataFrameCapableAdapter(Protocol):
    def fetch_dataframe(self, full_name: str, limit: int | None = None) -> pd.DataFrame: ...
    def write_dataframe(self, dataframe: pd.DataFrame, full_name: str, overwrite: bool = True) -> WriteResult: ...
```

This protocol lives in `src/clair/adapters/pandas_protocol.py` and is checked at runtime only when a PandasTrouve is encountered.

---

## 4. Runner Changes

### Branching point

The branching happens inside `run_project()` at the node execution level. The existing flow for each node is:

```
resolve_effective_mode -> build_sql -> execute statements
```

For PandasTrouve, the flow is:

```
fetch inputs as DataFrames -> call transform -> write result DataFrame
```

### Implementation: strategy dispatch in the main loop

```python
# In runner.py, inside run_project()

from clair.trouves.pandas_trouve import PandasTrouve

# ... inside the per-node loop, after context/schema setup ...

if isinstance(trouve, PandasTrouve):
    yield from _run_pandas_node(trouve, dag, adapter, name)
else:
    yield from _run_sql_node(trouve, dag, adapter, run_mode, run_id, name, ...)
```

The two helper functions:

```python
def _run_pandas_node(
    trouve: PandasTrouve,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    name: str,
) -> Iterator[RunResult]:
    """Execute a PandasTrouve: fetch -> transform -> write."""
    from clair.adapters.pandas_protocol import DataFrameCapableAdapter

    if not isinstance(adapter, DataFrameCapableAdapter):
        yield RunResult(
            full_name=name,
            status=RunStatus.FAILURE,
            error="Adapter does not support DataFrame operations (required for PandasTrouve)",
        )
        return

    start = time.monotonic()
    assert trouve.compiled is not None

    # 1. Fetch each input as a DataFrame
    input_frames: dict[str, pd.DataFrame] = {}
    for input_key, input_trouve_ref in trouve.inputs.items():
        # Resolve the input's full_name (it was compiled during discovery)
        input_full_name = input_trouve_ref.full_name
        try:
            input_frames[input_key] = adapter.fetch_dataframe(input_full_name)
        except Exception as e:
            duration = time.monotonic() - start
            yield RunResult(
                full_name=name,
                status=RunStatus.FAILURE,
                error=f"Failed to fetch input '{input_key}' ({input_full_name}): {e}",
                duration_seconds=duration,
            )
            return

    # 2. Call the transform function
    try:
        result_df = trouve.transform(input_frames)
    except Exception as e:
        duration = time.monotonic() - start
        yield RunResult(
            full_name=name,
            status=RunStatus.FAILURE,
            error=f"Transform function failed: {e}",
            duration_seconds=duration,
        )
        return

    # 3. Write the result back to Snowflake
    try:
        write_result = adapter.write_dataframe(
            dataframe=result_df,
            full_name=trouve.full_name,
            overwrite=True,
        )
    except Exception as e:
        duration = time.monotonic() - start
        yield RunResult(
            full_name=name,
            status=RunStatus.FAILURE,
            error=f"Failed to write result: {e}",
            duration_seconds=duration,
        )
        return

    duration = time.monotonic() - start

    if write_result.success:
        yield RunResult(
            full_name=name,
            status=RunStatus.SUCCESS,
            duration_seconds=duration,
        )
    else:
        yield RunResult(
            full_name=name,
            status=RunStatus.FAILURE,
            error=write_result.error,
            duration_seconds=duration,
        )
```

### Why branch inside `run_project()` rather than on the Trouve?

Putting execution logic on the model (e.g. `trouve.execute(adapter)`) would couple the model to the adapter. The runner is the right place for orchestration. The branch is a simple `isinstance` check -- minimal added complexity.

---

## 5. Discovery Changes

### Module-level variable convention

PandasTrouves use the **same `trouve = PandasTrouve(...)` convention**. Discovery already looks for `getattr(module, "trouve", None)`. The only change is the type check:

```python
# Current:
if not isinstance(trouve_obj, Trouve):
    continue

# New:
if not isinstance(trouve_obj, (Trouve, PandasTrouve)):
    continue
```

### Dependency detection for PandasTrouve

The existing `_detect_imports()` scans SQL for placeholder tokens. PandasTrouves have no SQL, so a parallel extraction is needed:

```python
def _detect_pandas_imports(
    inputs: dict[str, Any],
    id_to_full_name: dict[int, str],
) -> list[str]:
    """Extract full_names of upstream Trouves from PandasTrouve inputs."""
    imports = []
    for input_obj in inputs.values():
        dep_name = id_to_full_name.get(id(input_obj))
        if dep_name and dep_name not in imports:
            imports.append(dep_name)
    return imports
```

### CompiledAttributes for PandasTrouve

`CompiledAttributes.resolved_sql` will be **empty string** for PandasTrouves. This is consistent -- SOURCE Trouves also have no meaningful SQL. The field remains on the shared type. No structural changes to `CompiledAttributes` are needed.

### Full discovery flow change

In the compilation phase (Phase B) of `discover_project()`:

```python
for trouve_obj, full_name, file_path, module_name in collected:
    if isinstance(trouve_obj, PandasTrouve):
        imports = _detect_pandas_imports(trouve_obj.inputs, logical_names)
        resolved_sql = ""
    else:
        imports = _detect_imports(trouve_obj.sql, logical_names, logical)
        resolved_sql = _resolve_sql(trouve_obj.sql, logical_names)

    trouve_obj.compiled = CompiledAttributes(
        full_name=routed,
        logical_name=logical,
        resolved_sql=resolved_sql,
        file_path=file_path.relative_to(project_root),
        module_name=module_name,
        imports=imports,
        config=_resolve_config(file_path, project_root, profile_defaults),
    )
```

### `id_to_full_name` registry

The existing `logical_names` dict maps `id(trouve_obj) -> full_name`. Since PandasTrouve inputs hold *references* to the same Python objects that are in `collected`, the `id()` lookup works directly. No change to the registry mechanism.

---

## 6. Compile Output

### What `clair compile` writes for a PandasTrouve

There is no SQL to write. Instead, write a **JSON manifest** for each PandasTrouve node:

```
_clairtifacts/<run_id>/database/schema/table_name.json
```

Contents:

```json
{
  "node_type": "pandas",
  "full_name": "database.schema.table_name",
  "inputs": {
    "events": "database.refined.events",
    "users": "database.refined.users"
  },
  "transform_function": "compute_summary",
  "transform_module": "database.schema.table_name",
  "columns": [
    {"name": "event_type", "type": "STRING"},
    {"name": "total_events", "type": "NUMBER"}
  ],
  "docs": "Summary of events by type."
}
```

### Changes to `compiler.py`

- `CompiledNodeInfo` gets a new field: `node_type: str` (values: `"sql"` or `"pandas"`).
- For pandas nodes, `sql` is `[]` and `node_type` is `"pandas"`.
- The file-writing logic branches: `.sql` file for SQL nodes, `.json` manifest for pandas nodes.
- The render methods show `[pandas]` instead of SQL for pandas nodes:

```
--- database.schema.table_name [pandas] ---
Dependencies: database.refined.events, database.refined.users
Transform: compute_summary
Inputs: events -> database.refined.events, users -> database.refined.users
```

---

## 7. Public API Exports

Add to `src/clair/__init__.py`:

```python
from clair.trouves.pandas_trouve import PandasTrouve

__all__ = [
    # ... existing exports ...
    "PandasTrouve",
]
```

That's it. `PandasTrouve` is the only new public type. `WriteResult` and `DataFrameCapableAdapter` are internal.

---

## 8. New Files to Create

| File | Purpose |
|---|---|
| `src/clair/trouves/pandas_trouve.py` | `PandasTrouve` Pydantic model. Fields: `inputs`, `transform`, `columns`, `tests`, `docs`, `run_config`, `compiled`. Validators enforce TABLE-only, non-empty inputs, callable transform. Implements `__format__` for downstream SQL reference. |
| `src/clair/adapters/pandas_protocol.py` | `DataFrameCapableAdapter` runtime-checkable Protocol. Defines `fetch_dataframe()` and `write_dataframe()` signatures. `WriteResult` model. |
| `tests/unit/test_pandas_trouve.py` | Unit tests for `PandasTrouve` model construction, validation, and `__format__` behavior. |
| `tests/unit/test_pandas_discovery.py` | Unit tests for PandasTrouve discovery: dependency detection from `inputs`, CompiledAttributes generation, mixed DAGs with SQL and pandas nodes. |
| `tests/unit/test_pandas_runner.py` | Unit tests for PandasTrouve execution path in `run_project()`: mocked adapter, fetch/transform/write flow, error handling at each stage. |
| `tests/fixtures/pandas_project/` | Fixture project containing a SOURCE, a SQL TABLE, and a PandasTrouve that reads the TABLE. |

---

## 9. Files to Modify

| File | Changes |
|---|---|
| `src/clair/trouves/trouve.py` | No changes. `Trouve` is unchanged. |
| `src/clair/core/discovery.py` | Import `PandasTrouve`. Widen `isinstance` check to `(Trouve, PandasTrouve)`. Add `_detect_pandas_imports()`. Branch compilation phase for PandasTrouve (empty resolved_sql, inputs-based import detection). |
| `src/clair/core/dag.py` | Widen `ClairDag.validate()` assertion to accept `Trouve | PandasTrouve`. Widen `get_trouve()` return type annotation. `build_dag()` needs no change (it operates on `.compiled.imports` which both types provide). |
| `src/clair/core/runner.py` | Import `PandasTrouve`. Add `_run_pandas_node()` helper. Branch in `run_project()` main loop based on `isinstance`. |
| `src/clair/core/compiler.py` | Add `node_type` field to `CompiledNodeInfo`. Branch file output: `.sql` for SQL, `.json` manifest for pandas. Update render methods for pandas nodes. |
| `src/clair/adapters/snowflake.py` | Add `fetch_dataframe()` and `write_dataframe()` methods. Import `WriteResult` from `pandas_protocol.py` (or define locally and re-export). |
| `src/clair/adapters/base.py` | **No changes.** The ABC stays pandas-free. |
| `src/clair/__init__.py` | Add `PandasTrouve` to imports and `__all__`. |
| `pyproject.toml` | Add `pandas` as an optional dependency: `pandas = ["pandas>=2.0"]`. Keep it optional so pure-SQL projects don't need it. |

---

## 10. Trade-off Discussion

### Subclass vs composition for PandasTrouve vs Trouve

**Chosen: separate class (composition-style), no inheritance.**

| | Subclass | Separate class |
|---|---|---|
| **Code sharing** | Inherit `full_name`, `is_compiled`, `__format__` | Duplicate 3 small properties/methods (trivial) |
| **Validation** | Must override/extend parent validators carefully; easy to break invariants | Each class validates its own invariants cleanly |
| **Liskov substitution** | Violated -- `build_sql()` would need to raise `NotImplementedError` | No false promises; each type has exactly the interface it supports |
| **Discovery/runner changes** | `isinstance(trouve, Trouve)` works everywhere | Must check `isinstance(trouve, (Trouve, PandasTrouve))` |
| **Future node types** | Deep hierarchy risk | Extract a `Protocol` when the third type arrives |

The duplication cost is ~15 lines of property boilerplate. The benefit is that each class is self-contained and validates only its own invariants. This is the right trade-off at this stage.

**When to reconsider:** If a third node type (e.g. `SparkTrouve`, `DuckDBTrouve`) arrives, extract a `BaseTrouve` Protocol or ABC with the shared interface (`full_name`, `is_compiled`, `compiled`, `type`, `columns`, `tests`, `docs`).

### Inline pandas dep in adapter vs separate PandasAdapter layer

**Chosen: methods on SnowflakeAdapter, guarded by a Protocol.**

| | Separate PandasAdapter class | Methods on SnowflakeAdapter |
|---|---|---|
| **Purity** | ABC stays clean; pandas adapter wraps a WarehouseAdapter | ABC stays clean anyway (methods aren't abstract) |
| **Complexity** | Extra class, extra constructor arg, wrapper delegation | Two new methods on existing class |
| **Usage** | Runner needs both adapter and pandas_adapter | Runner has one adapter; checks Protocol at PandasTrouve execution time |
| **Testing** | Must mock two objects | Mock one object with extra methods |

A separate wrapper class adds indirection without adding value. The Protocol check gives us clean error messages when someone tries to run PandasTrouves against an adapter that doesn't support it (e.g., a future BigQuery adapter before DataFrame support is added).

### v1 incremental support: no

**PandasTrouve v1 is full-refresh only.** Incremental semantics for DataFrames are genuinely harder:

1. **APPEND**: We'd need to pass only "new" rows to `transform`. But what's "new"? There's no `{{ this }}` in pandas -- the user would need access to the existing table, a high-water mark, or a custom filter. The API surface would be large and error-prone.

2. **UPSERT**: We'd need to generate a MERGE statement from a DataFrame, which means knowing primary keys, generating staging tables, and handling schema evolution. This is the Snowflake `write_pandas` + MERGE dance -- doable but complex.

3. **User expectation**: Data scientists using PandasTrouve are doing complex transforms that don't map cleanly to append/upsert SQL semantics. Full refresh is the natural fit.

**When to add incremental:** When a user has a concrete use case (e.g., "I have a 100M row table and my pandas transform only needs the last day's data"). At that point, the API would be: PandasTrouve gets an optional `incremental_filter: Callable[[WarehouseAdapter, str], pd.DataFrame]` that replaces the default `SELECT *` fetch. This is a clean extension point that doesn't require rethinking the core model.

### pandas as optional dependency

**pandas is an optional extra**, not a core dependency. Rationale:
- Most Clair users write SQL Trouves. Forcing pandas on them adds install time and a large transitive dependency tree.
- `import pandas` is guarded at runtime: it only happens when a PandasTrouve is encountered in the project or when `fetch_dataframe`/`write_dataframe` is called.
- The import in `PandasTrouve.__init__` uses a lazy check:

```python
try:
    import pandas as pd
except ImportError:
    raise ImportError(
        "PandasTrouve requires pandas. Install it with: uv add 'clair[pandas]'"
    )
```

In `pyproject.toml`:

```toml
[project.optional-dependencies]
pandas = ["pandas>=2.0"]
```

---

## Appendix: Execution Flow Diagram

```
SQL Trouve:
  discover -> resolve placeholders -> build_sql() -> adapter.execute(sql)

PandasTrouve:
  discover -> extract inputs -> adapter.fetch_dataframe(input_1)
                              -> adapter.fetch_dataframe(input_2)
                              -> transform({input_1: df1, input_2: df2})
                              -> adapter.write_dataframe(result_df, target)
```

Both paths share:
- Discovery (file walk, module loading, config cascade)
- DAG construction (edges from `.compiled.imports`)
- Topological execution order
- Partial failure handling (skip downstream on error)
- Test execution (tests run against the materialized table via SQL)
- Selectors (`--select` filters on `full_name`)
- Compile output (artifacts directory)

---

## Appendix: Mixed DAG Example

```
SOURCE: raw.events (SQL)
    |
TABLE: refined.events (SQL)
    |
TABLE: derived.event_summary (PandasTrouve)
    |
TABLE: derived.summary_report (SQL -- reads from pandas-created table)
```

This works because:
1. `derived.event_summary` is a PandasTrouve that reads `refined.events` as a DataFrame.
2. It writes its result to `derived.event_summary` as a Snowflake table.
3. `derived.summary_report` is a SQL Trouve that references `derived.event_summary` via f-string -- which resolves to a real Snowflake table name.
4. The DAG correctly captures all edges. Topological sort ensures correct execution order.
