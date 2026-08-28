# Python API

```python
import clair

summary = clair.run("~/projects/analytics")
print(summary.succeeded_count, summary.failed_count)
```

Each operation of the CLI is one function of the package. The CLI parses the
arguments and calls these functions, so a notebook, a test, or another program
does the same work with no `subprocess` call.

| Function | The equivalent command |
|----------|------------------------|
| `clair.run()` | `clair run` |
| `clair.compile()` | `clair compile` |
| `clair.test()` | `clair test` |
| `clair.validate()` | `clair validate` |
| `clair.clean()` | `clair clean` |
| `clair.docs()` | `clair docs` |
| `clair.catalog()` | the data behind `clair docs` |

Each function gives a result object with the complete data of the operation. No
function writes to stdout, and no function stops the process: a fault raises a
`ClairError`.

## `clair.run()`

```python
def run(
    project_dir: str | Path = ".",
    *,
    select: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    env: str | None = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    test: bool = True,
    sample: bool = False,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> RunSummary
```

Run the Trouves on the warehouse, and test them. Clair writes each Trouve to a
run-scoped staging address, runs the tests there, and promotes the object after
the tests pass. See [Staging](../topics/staging.md). The tests give that
guarantee, so `test=False` also stops the staging step.

`threads` gives the number of Trouves that run at one time. None takes the
thread count of the environment. See
[Environments](../topics/environments.md).

Give `adapter` to keep one connection open for many calls. Clair then closes no
connection. The default makes a `SnowflakeAdapter`, connects it, and closes it.
A parallel run opens one more connection for each other thread, and it closes
each of those.

```python
import clair
from clair import RunMode

summary = clair.run(
    "~/projects/analytics",
    select=["+mydb.analytics.orders"],
    env="dev",
    run_mode=RunMode.INCREMENTAL,
)

for result in summary.failed:
    print(result.physical_address, result.error)
    print("\n\n".join(result.sql or []))
```

A Trouve that fails gives a `RunResult` with the `FAILURE` status, and raises no
error. The other branches of the DAG continue.

## `RunSummary`

```python
class RunSummary(BaseModel):
    results: list[RunResult]
    env_name: str
    run_id: str
    project_root: Path | None
    run_mode: RunMode
```

| Member | Gives |
|--------|-------|
| `results` | One `RunResult` for each Trouve, in the run order. |
| `result(address)` | One `RunResult` by its logical address or its physical address. `None` if the run holds no such Trouve. |
| `succeeded`, `failed`, `skipped` | The results with that status. |
| `succeeded_count`, `failed_count`, `skipped_count` | The number of results with that status. |
| `test_results` | Each `TestResult` of the run, in the run order. |
| `render()` | The summary text that the CLI shows. |

## `RunResult`

```python
class RunResult(BaseModel):
    logical_address: str
    physical_address: str
    status: RunStatus
    query_ids: list[str]
    query_urls: list[str]
    staging_address: str | None
    effective_run_mode: RunMode | None
    error: str
    sql: list[str] | None
    failed_statement_index: int | None
    duration_seconds: float
    row_count: int
    test_results: list[TestResult]
    skipped_by: str | None
```

| Attribute | Gives |
|-----------|-------|
| `logical_address` | The name that the file path gives. |
| `physical_address` | The name that clair writes to, after routing. |
| `status` | `RunStatus.SUCCESS`, `RunStatus.FAILURE` or `RunStatus.SKIPPED`. |
| `query_ids`, `query_urls` | The warehouse query ID and console URL of each statement. |
| `staging_address` | The run-scoped address that clair built at. `None` without staging. |
| `effective_run_mode` | The mode that clair used, after the `RunConfig` of the Trouve and the fallback to a full refresh. See [Incrementality](../topics/incrementality.md). |
| `sql` | The statements, in the order that clair executed them. |
| `failed_statement_index` | The index in `sql` of the statement that failed. |
| `duration_seconds` | The clock time of the statements. |
| `row_count` | The rows that the last build statement changed. |
| `test_results` | The data quality test results of this Trouve. |
| `skipped_by` | The physical address of the upstream Trouve that caused the skip. |

## `clair.compile()`

```python
def compile(
    project_dir: str | Path = ".",
    *,
    select: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    env: str | None = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    use_staging: bool = True,
) -> CompileOutput
```

Compile the project and write the SQL to `_clairtifacts/<run_id>/`. This needs no
warehouse connection.

```python
output = clair.compile("~/projects/analytics")
node = output.node("mydb.analytics.orders")
print(node.physical_address, node.staging_address)
print("\n\n".join(node.sql))
```

`CompileOutput` holds `compiled_nodes`, `trouve_count`, `source_count`,
`artifacts_dir`, `run_id`, `project_root`, `env_name` and `run_mode`. The method
`node(address)` finds one `CompiledNodeInfo` by its logical address or its
physical address.

`CompiledNodeInfo` holds `name`, `logical_address`, `physical_address`,
`staging_address`, `type`, `execution_type`, `effective_run_mode`,
`dependencies`, `sql` and `artifact_path`.

## `clair.test()`

```python
def test(
    project_dir: str | Path = ".",
    *,
    select: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    env: str | None = None,
    sample: bool = False,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> TestSummary
```

Run the data quality tests on the warehouse. See
[Data Quality Tests](../topics/data-quality-tests.md).

`TestSummary` holds `results`, and gives `passed_count`, `failed_count`,
`error_count`, `passed_results`, `failed_results` and `render()`.

```python
summary = clair.test("~/projects/analytics", sample=True)
for result in summary.failed_results:
    print(result.physical_address, result.test_type, result.failing_row_count)
```

## `clair.validate()`

```python
def validate(
    project_dir: str | Path = ".",
    *,
    env: str | None = None,
) -> ValidationReport
```

Apply the project routing entries to each Trouve. This function needs no
connection. See [`clair validate`](../cli/validate.md).

`ValidationReport` holds `env_name`, `routing_file`, `routing_description`,
`routable_count`, `address_problems`, `collisions`, `text_references` and
`unnamed_environment_warning`. It gives `problem_count`, `is_valid` and
`render()`.

Each problem is an object, thus a caller reads the Trouve that holds it:

```python
report = clair.validate("~/projects/analytics")
for collision in report.collisions:
    print(collision.physical_address, collision.logical_addresses)
for problem in report.address_problems:
    print(problem.logical_address, problem.detail)
```

## `clair.clean()`

```python
def clean(
    project_dir: str | Path = ".",
    *,
    before: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> CleanOutput
```

Remove the compiled artifacts of the old runs. This function needs no
connection. See [`clair clean`](../cli/clean.md).

`before` accepts `today`, `yesterday`, `last_week`, a duration such as `7d`,
`24h` or `30m`, or an ISO date such as `2026-03-01`. `None` removes each run.
`now` gives the current time for that value, thus a test needs no clock.

`CleanOutput` holds `artifacts_dir`, `artifacts_dir_exists`, `cutoff`, `runs`
and `dry_run`. It gives `run_count` and `run_ids`.

```python
plan = clair.clean("~/projects/analytics", before="7d", dry_run=True)
print(plan.run_count, plan.run_ids)
```

## `clair.docs()` and `clair.catalog()`

```python
def docs(
    project_dir: str | Path = ".",
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None

def catalog(project_dir: str | Path = ".") -> dict
```

`docs()` starts the local web UI. The function does not give control back.

`catalog()` gives the documentation data that the UI shows: one entry for each
Trouve, and the lineage edges. It needs no connection, and it is the one
operation with no command of its own.

## Logs

The functions write structured logs with `structlog`. The CLI configures the
renderer. A notebook that wants the same output calls the configuration once:

```python
from clair._logging import configure_logging

configure_logging()
```

A notebook that wants fewer messages raises the level in place of that call:

```python
import logging

import structlog

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))
```

## Notebooks

`examples/notebooks/` holds four notebooks that run each operation above. None of
them opens a warehouse connection:

| Notebook | Shows |
|----------|-------|
| `01_python_api_tour.ipynb` | `compile()`, `validate()`, `catalog()` and `clean()`, with the selectors and the run modes |
| `02_lineage_and_impact.ipynb` | the DAG as a networkx graph: the blast radius of a source, the build order, and the parallel groups |
| `03_run_without_snowflake.ipynb` | `run()` against a `WarehouseAdapter` that holds its tables in memory |
| `04_author_trouves.ipynb` | a `PandasTrouve` transform under test, a `SeedTrouve`, and a project that the notebook writes |
