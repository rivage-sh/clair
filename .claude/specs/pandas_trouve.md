# PandasTrouve — Product Spec

## Problem statement

Data science workflows routinely mix SQL and pandas: query from Snowflake, manipulate as a DataFrame, write back. Today this step lives entirely outside clair's DAG: it's a one-off script with no lineage, no tests, and no relationship to the SQL Trouves that feed or consume it. When upstream data changes, the script must be manually re-run. There's no way to `clair run` a pipeline that includes pandas transformations.

## Goals

- First-class `PandasTrouve` that accepts a Python function `(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame`
- Clair fetches upstream data, calls the function, writes the result back to Snowflake
- The DAG, lineage, `--select` filtering, and test framework work unchanged across SQL and pandas nodes
- `PandasTrouve` nodes appear in `clair dag` output
- Existing tests (TestUnique, TestNotNull, TestRowCount, TestUniqueColumns) apply to the output table exactly as for SQL Trouves

## Non-goals (v1)

- Incremental materializations for pandas (full-refresh only; incremental semantics are genuinely complex)
- Chunked reads for large tables (fetch full table into memory; defer chunking to v2)
- VIEW type for PandasTrouve (output is always a TABLE)
- SOURCE type for PandasTrouve (nonsensical)
- Custom write modes (always CREATE OR REPLACE)

## User-facing API

```python
# example_1_database/derived/pandas_summary.py
import pandas as pd
from clair import PandasTrouve, Column, ColumnType
from example_1_database.refined.events import trouve as example_1_database_refined_events

def summarize(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    events = inputs["events"]
    return (
        events
        .groupby("event_type")["event_date"]
        .count()
        .reset_index()
        .rename(columns={"event_date": "event_count"})
    )

trouve = PandasTrouve(
    inputs={"events": example_1_database_refined_events},
    transform=summarize,
    columns=[
        Column(name="event_type", type=ColumnType.STRING),
        Column(name="event_count", type=ColumnType.NUMBER),
    ],
    docs="Summary of event counts by type, computed in pandas.",
)
```

Key points:
- `inputs` maps string keys to upstream Trouve objects (imports as usual)
- `transform` is a plain Python function; keys in `inputs` become keys in `inputs` dict passed to it
- `columns` is optional but recommended for documentation and schema enforcement
- `tests` works identically to SQL Trouves
- No `sql` field; no `type` field (always TABLE); no `run_config` (always full-refresh)

## Behavior at `clair run`

For each `PandasTrouve` node (in topological order):

1. **Fetch**: For each entry in `inputs`, execute `SELECT * FROM {full_name}` and load into a pandas DataFrame
2. **Transform**: Call `transform({"key": df, ...})` with the loaded DataFrames
3. **Write**: Write the returned DataFrame back to Snowflake as the output table (CREATE OR REPLACE semantics)
4. **Test**: Run attached tests against the output table (same as SQL Trouves)

If `transform` raises, the node fails and all downstream nodes are skipped (same partial-failure semantics as SQL Trouves).

## Incremental semantics

Not supported in v1. Any `run_config` on a PandasTrouve would be rejected at construction time. The effective run mode is always FULL_REFRESH.

## DAG integration

Dependencies declared via `inputs={"name": trouve}` dict. Discovery extracts dependencies by inspecting the id() of each input value against the in-memory registry (same mechanism as SQL placeholder tokens). No function execution needed during discovery.

`clair dag` renders PandasTrouve nodes with a `[pandas]` type annotation.

## Compile output

`clair compile` writes a `.json` manifest per PandasTrouve node instead of `.sql`:

```json
{
  "type": "pandas",
  "full_name": "db.schema.table",
  "inputs": {"events": "db.refined.events"},
  "transform_fn": "summarize",
  "source_file": "db/schema/table.py",
  "columns": [...]
}
```

## Error handling

| Scenario | Behavior |
|---|---|
| `transform` raises an exception | Node marked failed; downstream skipped; exception logged with traceback |
| Input fetch fails (Snowflake error) | Node marked failed; downstream skipped |
| `transform` returns non-DataFrame | Node marked failed with clear error message |
| pandas not installed | `ImportError` at `PandasTrouve(...)` construction time with message pointing to `pip install clair[pandas]` |
| Output write fails | Node marked failed; downstream skipped |

## Acceptance criteria

- [ ] `PandasTrouve` importable from `clair` package
- [ ] `clair compile` succeeds for a project with PandasTrouve nodes (no Snowflake connection)
- [ ] `clair compile` writes a `.json` manifest for each PandasTrouve node
- [ ] `clair dag` renders PandasTrouve nodes in topological order
- [ ] `clair run` fetches all inputs, calls transform, writes result to Snowflake
- [ ] `clair test` runs attached tests against PandasTrouve output table
- [ ] Failed transform marks node failed and skips downstream (not unrelated branches)
- [ ] `--select` filtering works on PandasTrouve nodes
- [ ] pandas not installed → clear ImportError at construction time
- [ ] `transform` returning non-DataFrame → clear error at run time
- [ ] PandasTrouve with no `columns` → works (columns optional)
- [ ] Existing SQL Trouves can depend on a PandasTrouve output (normal upstream/downstream)
- [ ] PandasTrouve can depend on another PandasTrouve
- [ ] Unit tests cover: model validation, dependency extraction in discovery, runner branching
