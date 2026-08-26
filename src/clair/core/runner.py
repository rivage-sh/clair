"""The runner. It executes the DAG on Snowflake in dependency order."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from typing import NamedTuple

import networkx as nx
import pandas as pd
import structlog
from pydantic import BaseModel, model_validator

from clair.adapters.base import WarehouseAdapter
from clair.adapters.pool import AdapterPool
from clair.core.dag import ClairDag, get_executable_nodes
from clair.core.staging import (
    build_clone_statement,
    build_drop_staging_statement,
    build_promote_statement,
    make_staging_address,
)
from clair.environments.routing import TrouveAddress
from clair.exceptions import ClairError, RunError
from clair.trouves.pandas_trouve import PandasTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, Trouve, TrouveType


class RunStatus(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"


class RunResult(BaseModel):
    """The result after clair materializes one Trouve in the warehouse.

    Attributes:
        logical_address: The name that the file path gives. The DAG edges, the
            selectors and the Trouve files use it.
        physical_address: The name that clair writes to. A routing entry makes it
            from the logical address. The two are equal without an entry.
        status: The result of the attempt.
        query_ids: The warehouse query ID of each statement.
        query_urls: The URL of each statement in the Snowflake console.
        error: The error message if the query failed.
        sql: The complete DDL. Clair sets it only for a FAILURE.
        duration_seconds: The clock time of the query.
        skipped_by: The physical_address of the upstream Trouve that caused the skip.
            Clair sets it only for a SKIPPED result.
    """

    logical_address: str = ""
    physical_address: str
    status: RunStatus
    query_ids: list[str] = []
    query_urls: list[str] = []
    error: str = ""
    sql: list[str] | None = None
    duration_seconds: float = 0.0
    skipped_by: str | None = None

    @model_validator(mode='after')
    def _check_skipped_has_cause(self) -> RunResult:
        if self.status == RunStatus.SKIPPED and not self.skipped_by:
            raise ValueError("a SKIPPED result must have skipped_by")
        return self


def _append_query_urls(lines: list[str], query_ids: list[str], query_urls: list[str]) -> None:
    """Add a query ID line and a URL line. Add an [i/n] label if there are 2 or more statements."""
    n = len(query_ids)
    for i, (qid, url) in enumerate(zip(query_ids, query_urls), 1):
        prefix = f" [{i}/{n}]" if n > 1 else ""
        lines.append(f"      Query ID{prefix}: {qid}")
        lines.append(f"      URL{prefix}: {url}")


class RunSummary(BaseModel):
    """The result of one run operation."""

    results: list[RunResult]
    env_name: str

    @property
    def succeeded_count(self) -> int:
        return sum(1 for r in self.results if r.status == RunStatus.SUCCESS)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == RunStatus.FAILURE)

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.results if r.status == RunStatus.SKIPPED)

    @property
    def succeeded(self) -> list[RunResult]:
        return [r for r in self.results if r.status == RunStatus.SUCCESS]

    @property
    def failed(self) -> list[RunResult]:
        return [r for r in self.results if r.status == RunStatus.FAILURE]

    @property
    def skipped(self) -> list[RunResult]:
        return [r for r in self.results if r.status == RunStatus.SKIPPED]

    @staticmethod
    def render_header(total: int, env_name: str) -> str:
        """Make the run header. Clair shows it before the first node starts."""
        return (
            f"=== Clair Run (env: {env_name}) ===\n"
            f"\n"
            f"Clair runs {total} Trouve{'s' if total != 1 else ''}...\n"
        )

    @staticmethod
    def render_node(result: RunResult, index: int, total: int) -> str:
        """Make the output lines of one node that completed."""
        lines: list[str] = []

        if result.status == RunStatus.SKIPPED:
            lines.append(f"[{index}/{total}] {result.physical_address} ... SKIPPED")
            lines.append(f"      Reason: the upstream dependency {result.skipped_by} failed")
        elif result.status == RunStatus.SUCCESS:
            lines.append(
                f"[{index}/{total}] {result.physical_address} ... OK ({result.duration_seconds:.1f}s)"
            )
            _append_query_urls(lines, result.query_ids, result.query_urls)
        elif result.status == RunStatus.FAILURE:
            lines.append(
                f"[{index}/{total}] {result.physical_address} ... FAILED ({result.duration_seconds:.1f}s)"
            )
            _append_query_urls(lines, result.query_ids, result.query_urls)
            lines.append(f"      Error: {result.error}")
            if result.sql:
                lines.append("      SQL:")
                for stmt in result.sql:
                    for sql_line in stmt.strip().splitlines():
                        lines.append(f"        {sql_line}")
                    lines.append("")
        else:
            raise NotImplementedError()

        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def render_footer(succeeded: int, failed: int, skipped: int) -> str:
        """Make the last line of the summary."""
        return f"=== Done: {succeeded} succeeded, {failed} failed, {skipped} skipped ==="

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        total = len(self.results)
        parts = [self.render_header(total, self.env_name)]

        for i, r in enumerate(self.results, 1):
            parts.append(self.render_node(r, i, total))

        parts.append(self.render_footer(
            self.succeeded_count, self.failed_count, self.skipped_count,
        ))

        return "\n".join(parts)


logger = structlog.get_logger()


def resolve_effective_mode(trouve: Trouve, cli_run_mode: RunMode) -> RunMode:
    """Find the effective run mode of a Trouve. This function needs no adapter.

    The compiler and the runner both call it. The compiler has no connection.
    The runner also asks the warehouse if the table exists, before it accepts
    the incremental mode.
    """
    if trouve.type == TrouveType.VIEW:
        return RunMode.FULL_REFRESH
    if trouve.run_config.run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    if cli_run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    return RunMode.INCREMENTAL


def _run_pandas_trouve(
    trouve: PandasTrouve,
    adapter: WarehouseAdapter,
    physical_address: TrouveAddress,
    staging_address: TrouveAddress | None = None,
) -> RunResult:
    """Execute a PandasTrouve. Read the inputs, transform them, write the output.

    Args:
        trouve: The Trouve to execute.
        adapter: A warehouse adapter with an open connection.
        physical_address: The address of the Trouve in the warehouse.
        staging_address: The staging address, if the run has a staging step.
            The DataFrame goes there, and not to the physical address.

    Returns a RunResult with the SUCCESS status or the FAILURE status.
    """
    start = time.monotonic()

    # 1. Read each input DataFrame. Clair keeps the order of trouve.inputs.
    input_dataframes: list[pd.DataFrame] = []
    # compiled.input_addresses holds the address of each input, in the parameter
    # order of the transform. recompile_for_selection() sets it: the physical
    # address of an input that this run builds, and the logical production
    # address of an input that it does not build.
    assert trouve.compiled is not None
    assert len(trouve.compiled.input_addresses) == len(trouve.inputs)
    for parameter_name, input_address in zip(
        trouve.parameter_names(), trouve.compiled.input_addresses
    ):
        try:
            input_dataframes.append(
                adapter.fetch_dataframe(TrouveAddress.parse(input_address))
            )
        except Exception as fetch_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
            duration = time.monotonic() - start
            return RunResult(
                physical_address=str(trouve.physical_address),
                status=RunStatus.FAILURE,
                error=f"Clair cannot read the input '{parameter_name}' ({input_address}): {fetch_error}",
                duration_seconds=duration,
            )

    # 2. Call the transform function. Clair binds each input by position.
    try:
        result_dataframe = trouve.transform(*input_dataframes)
    except Exception as transform_error:  # noqa: BLE001 — the user transform code is unknown
        duration = time.monotonic() - start
        return RunResult(
            physical_address=str(trouve.physical_address),
            status=RunStatus.FAILURE,
            error=f"The transform function failed: {transform_error}",
            duration_seconds=duration,
        )

    # 3. Make sure that the result is a DataFrame.
    if not isinstance(result_dataframe, pd.DataFrame):
        duration = time.monotonic() - start
        return RunResult(
            physical_address=str(trouve.physical_address),
            status=RunStatus.FAILURE,
            error=(
                f"The transform function must return a pandas DataFrame, "
                f"but it returned {type(result_dataframe).__name__}"
            ),
            duration_seconds=duration,
        )

    # 4. Write the result to Snowflake. A TrouveAddress is valid when it exists,
    # so the three names need no test here.
    address = staging_address or physical_address

    try:
        query_result = adapter.write_dataframe(dataframe=result_dataframe, address=address)
    except Exception as write_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
        duration = time.monotonic() - start
        return RunResult(
            physical_address=str(physical_address),
            status=RunStatus.FAILURE,
            error=f"Clair cannot write the DataFrame to {address}: {write_error}",
            duration_seconds=duration,
        )

    duration = time.monotonic() - start

    if not query_result.success:
        return RunResult(
            physical_address=str(physical_address),
            status=RunStatus.FAILURE,
            error=query_result.error or "write_dataframe returned success=False",
            duration_seconds=duration,
        )

    return RunResult(
        physical_address=str(physical_address),
        status=RunStatus.SUCCESS,
        query_ids=[query_result.query_id] if query_result.query_id else [],
        query_urls=[query_result.query_url] if query_result.query_url else [],
        duration_seconds=duration,
    )


def _promote_or_keep(
    trouve: Trouve | PandasTrouve,
    adapter: WarehouseAdapter,
    staging_address: TrouveAddress,
    physical_address: TrouveAddress,
    tests_passed: bool,
) -> tuple[list[str], list[str], str]:
    """Complete a node: promote the staging object, or keep it for an examination.

    Args:
        trouve: The Trouve that clair materialized at the staging address.
        adapter: A warehouse adapter with an open connection.
        staging_address: The address that holds the candidate data.
        physical_address: The address that the data must reach.
        tests_passed: The result of the data quality tests on the staging object.

    Returns:
        The query IDs, the query URLs, and an error message. The error message is
        empty when the physical address holds the tested data.
    """
    query_ids: list[str] = []
    query_urls: list[str] = []

    def execute(statement: str) -> str:
        """Execute one statement. Give the error message, or an empty string."""
        query_result = adapter.execute(statement)
        if query_result.query_id:
            query_ids.append(query_result.query_id)
        if query_result.query_url:
            query_urls.append(query_result.query_url)
        return "" if query_result.success else (query_result.error or "unknown error")

    if not tests_passed:
        return (
            query_ids,
            query_urls,
            (
                f"the tests failed. {physical_address} keeps its data. "
                f"The rejected candidate stays at {staging_address}"
            ),
        )

    assert trouve.compiled is not None
    promote_error = execute(
        build_promote_statement(
            trouve.type,
            staging_address=staging_address,
            physical_address=physical_address,
            resolved_sql=trouve.compiled.resolved_sql,
        )
    )
    if promote_error:
        return (
            query_ids,
            query_urls,
            (
                f"the tests passed, but the promotion failed: {promote_error}. "
                f"The candidate stays at {staging_address}"
            ),
        )

    # The physical address now holds the tested data, so the staging copy has no
    # more use. A fault here makes clutter, not a wrong table, thus the node
    # keeps the SUCCESS status.
    drop_result = adapter.execute(build_drop_staging_statement(trouve.type, staging_address))
    if not drop_result.success:
        logger.warning(
            "run.node.staging_drop_failed",
            trouve=str(physical_address),
            staging=str(staging_address),
            error=drop_result.error,
        )

    return query_ids, query_urls, ""


def _logical_address_of(dag: ClairDag, physical_address: str) -> str:
    """Give the logical address of the DAG node that *physical_address* keys.

    Give the physical address back if the node holds no compiled attributes.
    """
    trouve = dag.get_trouve(physical_address)
    if trouve.compiled is None:
        return physical_address
    return str(trouve.compiled.logical_address)


class _NodeOutcome(NamedTuple):
    """The report of one node to the scheduler.

    Attributes:
        result: The result to give to the caller. It is None when the Trouve
            makes no statement, because clair then has nothing to tell.
        downstream_ok: False when clair must skip each node downstream. A node
            that fails gives False. A node that succeeds, but does not pass its
            tests, gives False too.
    """

    result: RunResult | None
    downstream_ok: bool


def _make_databases_and_schemas(
    dag: ClairDag, selected: list[str], adapter: WarehouseAdapter
) -> None:
    """Make each database and each schema that the run writes to.

    Clair does this one time, before the threads start. Many Trouves write to
    one schema. Concurrent CREATE SCHEMA IF NOT EXISTS statements against that
    schema can collide in the warehouse, and this function prevents it.
    """
    made: set[tuple[str, str]] = set()

    for name in selected:
        trouve = dag.get_trouve(name)
        if trouve.type == TrouveType.SOURCE:
            continue

        assert trouve.compiled is not None
        physical_address = trouve.compiled.physical_address
        key = (physical_address.database_name, physical_address.schema_name)
        if key in made:
            continue
        made.add(key)

        adapter.execute(f"CREATE DATABASE IF NOT EXISTS {physical_address.database_name}")
        adapter.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{physical_address.database_name}.{physical_address.schema_name}"
        )


def _run_node(
    name: str,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    run_mode: RunMode,
    run_id: str,
    after_node_success: Callable[[str, str, WarehouseAdapter], bool] | None,
    use_staging: bool,
) -> _NodeOutcome:
    """Materialize one Trouve in the warehouse.

    A thread of the run calls this function. *adapter* is the private connection
    of that thread, and the function gives the same adapter to each callback.

    Args:
        name: The physical address of the node, which is the key in the DAG.
        dag: The project DAG.
        adapter: The warehouse adapter of the thread that calls this function.
        run_mode: The run mode that the command line gives.
        run_id: The identifier of the run. A staging address holds it.
        after_node_success: The callback that runs the data quality tests.
        use_staging: True if clair writes to a staging address first.

    Returns:
        A _NodeOutcome. It holds the result, and it tells you if clair can
        continue with the nodes downstream.
    """
    # Each DAG node has the physical address as its key. The logs and the
    # results show both names, thus the reader sees the file that made the
    # Trouve, and the object that clair writes.
    logical_address = _logical_address_of(dag, name)
    trouve = dag.get_trouve(name)
    assert trouve.compiled is not None

    context_warehouse = trouve.compiled.config.warehouse if trouve.compiled.config.warehouse and trouve.compiled.config.warehouse.strip() else None
    context_role = trouve.compiled.config.role if trouve.compiled.config.role and trouve.compiled.config.role.strip() else None
    if context_warehouse or context_role:
        try:
            adapter.set_context(warehouse=context_warehouse, role=context_role)
        except Exception as e:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
            logger.warning("run.node.context_error", logical=logical_address, physical=name, warehouse=context_warehouse, role=context_role, error=str(e))
            return _NodeOutcome(
                RunResult(
                    logical_address=logical_address,
                    physical_address=name,
                    status=RunStatus.FAILURE,
                    error=f"Clair cannot set the session context: {e}",
                ),
                downstream_ok=False,
            )

    physical_address = trouve.compiled.physical_address

    # A staged run materializes the Trouve at a run-scoped address beside the
    # physical one. Clair writes the physical address only after the tests on
    # that object pass.
    staging_address = None
    if use_staging:
        try:
            staging_address = make_staging_address(physical_address, run_id)
        except ClairError as naming_error:
            logger.warning("run.node.failure", logical=logical_address, physical=name, error=str(naming_error))
            return _NodeOutcome(
                RunResult(
                    logical_address=logical_address,
                    physical_address=name,
                    status=RunStatus.FAILURE,
                    error=str(naming_error),
                ),
                downstream_ok=False,
            )

    # A PandasTrouve is different. Clair reads the data, transforms it and
    # writes it. Clair does not execute SQL.
    if trouve.execution_type == ExecutionType.PANDAS:
        assert isinstance(trouve, PandasTrouve)
        logger.info("run.node.start", logical=logical_address, physical=name, effective_mode="full_refresh")
        result = _run_pandas_trouve(trouve, adapter, physical_address, staging_address)

        downstream_ok = True

        if result.status == RunStatus.SUCCESS and staging_address is not None:
            assert after_node_success is not None
            tests_passed = after_node_success(name, str(staging_address), adapter)
            promote_ids, promote_urls, staging_error = _promote_or_keep(
                trouve, adapter, staging_address, physical_address, tests_passed
            )
            result = result.model_copy(
                update={
                    "query_ids": result.query_ids + promote_ids,
                    "query_urls": result.query_urls + promote_urls,
                    "status": RunStatus.FAILURE if staging_error else result.status,
                    "error": staging_error or result.error,
                }
            )
        elif result.status == RunStatus.FAILURE and staging_address is not None:
            result = result.model_copy(
                update={
                    "error": (
                        f"{result.error}. Clair keeps the staging object at "
                        f"{staging_address}, if the write made one"
                    )
                }
            )

        result = result.model_copy(update={"logical_address": logical_address})

        if result.status == RunStatus.SUCCESS:
            logger.info("run.node.success", logical=logical_address, physical=name, duration_seconds=round(result.duration_seconds, 3))
            # Without staging the tests run after clair wrote the physical object.
            if staging_address is None and after_node_success is not None:
                downstream_ok = after_node_success(name, str(physical_address), adapter)
        else:
            logger.warning("run.node.failure", logical=logical_address, physical=name, duration_seconds=round(result.duration_seconds, 3), error=result.error)
            downstream_ok = False

        return _NodeOutcome(result, downstream_ok)

    assert isinstance(trouve, Trouve)
    effective_mode = resolve_effective_mode(trouve, run_mode)
    # If the target table does not exist yet, change to the full refresh mode.
    if effective_mode == RunMode.INCREMENTAL:
        table_exists = adapter.table_exists(
            physical_address.database_name,
            physical_address.schema_name,
            physical_address.table_name,
        )
        if not table_exists:
            logger.info("run.node.incremental_fallback", logical=logical_address, physical=name, reason="table_not_found")
            effective_mode = RunMode.FULL_REFRESH

    logger.info("run.node.start", logical=logical_address, physical=name, effective_mode=effective_mode.value)
    statements = trouve.build_sql(effective_mode, run_id, staging_address=staging_address)

    # An incremental run changes data that already exists, so the staging
    # table needs that data first. A zero-copy clone gives it in constant time.
    if staging_address is not None and effective_mode == RunMode.INCREMENTAL:
        statements = [build_clone_statement(physical_address, staging_address)] + statements

    if not statements:
        return _NodeOutcome(None, downstream_ok=True)

    start = time.monotonic()
    last_result = None
    all_succeeded = True
    failed_at = None
    query_ids: list[str] = []
    query_urls: list[str] = []

    for stmt_idx, stmt in enumerate(statements):
        query_result = adapter.execute(stmt)
        last_result = query_result
        if query_result.query_id:
            query_ids.append(query_result.query_id)
        if query_result.query_url:
            query_urls.append(query_result.query_url)
        if not query_result.success:
            all_succeeded = False
            failed_at = stmt_idx
            break

    duration = time.monotonic() - start

    # UPSERT cleanup. The UPSERT mode always ends with these three statements:
    # make the merge table, merge it, drop it. If the merge failed, drop the
    # merge table anyway. The index comes from the end of the list, because a
    # staged incremental run puts a clone in front of the three.
    if not all_succeeded and len(statements) >= 3:
        merge_index = len(statements) - 2
        drop_index = len(statements) - 1
        if failed_at == merge_index:
            adapter.execute(statements[drop_index])

    # A staged run tests the staging object, then promotes it or keeps it.
    staging_error = ""
    if all_succeeded and staging_address is not None:
        assert after_node_success is not None
        tests_passed = after_node_success(name, str(staging_address), adapter)
        promote_ids, promote_urls, staging_error = _promote_or_keep(
            trouve, adapter, staging_address, physical_address, tests_passed
        )
        query_ids.extend(promote_ids)
        query_urls.extend(promote_urls)
        all_succeeded = not staging_error

    if all_succeeded:
        logger.info("run.node.success", logical=logical_address, physical=name, duration_seconds=round(duration, 3), query_ids=query_ids)
        downstream_ok = True
        # Without staging the tests run after clair wrote the physical object.
        if staging_address is None and after_node_success is not None:
            downstream_ok = after_node_success(name, str(physical_address), adapter)
        return _NodeOutcome(
            RunResult(
                logical_address=logical_address,
                physical_address=name,
                status=RunStatus.SUCCESS,
                query_ids=query_ids,
                query_urls=query_urls,
                duration_seconds=duration,
            ),
            downstream_ok,
        )

    if staging_error:
        error_message = staging_error
    else:
        assert last_result is not None
        error_message = last_result.error or ""
        if staging_address is not None:
            error_message = (
                f"{error_message}. Clair keeps the staging object at "
                f"{staging_address}, if the build made one"
            )
    logger.warning("run.node.failure", logical=logical_address, physical=name, duration_seconds=round(duration, 3), error=error_message, query_ids=query_ids)
    return _NodeOutcome(
        RunResult(
            logical_address=logical_address,
            physical_address=name,
            status=RunStatus.FAILURE,
            query_ids=query_ids,
            query_urls=query_urls,
            error=error_message,
            sql=statements,
            duration_seconds=duration,
        ),
        downstream_ok=False,
    )


def run_project(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    after_node_success: Callable[[str, str, WarehouseAdapter], bool] | None = None,
    use_staging: bool = False,
    threads: int = 1,
) -> Iterator[RunResult]:
    """Execute the selected Trouves. Give each result when the Trouve completes.

    Clair starts a Trouve when each Trouve that it imports completed. With more
    than one thread, clair starts as many Trouves at one time as the thread
    count permits. Each thread holds a private warehouse connection.

    If a node fails, clair marks each node downstream of it as skipped. Then
    clair continues with the other branches.

    The results come in completion order, not in topological order. A slow
    Trouve that starts first can thus arrive after a quick Trouve. Sort the
    results against `get_execution_order(dag)` if you need the DAG order.

    Args:
        dag: The project DAG.
        selected: The names of the Trouves to run.
        adapter: A warehouse adapter with an open connection. Clair uses it as
            the first connection of the pool, and opens `threads - 1` more.
        run_mode: The run mode that the command line gives.
        run_id: The identifier of the run. A staging address holds it.
        after_node_success: an optional callback. Clair calls it after each node
            that succeeds, before the promotion. The three arguments are the
            node name, the address that holds the new data, and the connection
            of the thread. Give False to make clair treat the node as a failure
            and skip each node downstream of it. This stops the run early when
            a test fails.
        use_staging: if True, clair writes each node to a run-scoped staging
            address, runs the tests there, and promotes the object only after
            the tests pass. The tests decide the promotion, so this needs
            after_node_success.
        threads: The number of Trouves that clair runs at one time, and thus the
            number of warehouse connections.

    Raises:
        RunError: If *use_staging* is True and *after_node_success* is None.
        ValueError: If *threads* is less than 1.
    """
    if use_staging and after_node_success is None:
        raise RunError(
            "A staged run needs the tests. Give after_node_success, because the "
            "tests decide if clair promotes a Trouve."
        )
    if threads < 1:
        raise ValueError(f"The thread count must be 1 or more, but it is {threads}")

    all_executable = get_executable_nodes(dag)
    to_run = [name for name in all_executable if name in selected]
    if not to_run:
        return

    _make_databases_and_schemas(dag, to_run, adapter)

    # Each node of this run that is downstream of *name*, direct or not. A node
    # that the selector removed is not in the map, but clair looks through it:
    # with A -> B -> C, and B removed, C is still downstream of A. Thus C waits
    # for A, and a failure of A still skips C.
    to_run_set = set(to_run)
    downstream_nodes = {
        name: nx.descendants(dag, name) & to_run_set for name in to_run
    }

    # The number of Trouves upstream of each node that did not complete yet.
    unmet_count = dict.fromkeys(to_run, 0)
    for downstream_of_name in downstream_nodes.values():
        for downstream in downstream_of_name:
            unmet_count[downstream] += 1

    # to_run is in topological order, thus the first ready nodes keep that order.
    ready = deque(name for name in to_run if unmet_count[name] == 0)
    skip_reasons: dict[str, str] = {}

    def release(name: str) -> None:
        """Tell each node downstream that *name* completed. At zero it is ready."""
        for downstream in downstream_nodes[name]:
            unmet_count[downstream] -= 1
            if unmet_count[downstream] == 0:
                ready.append(downstream)

    pool = AdapterPool(adapter, threads)

    def run_one(name: str) -> _NodeOutcome:
        """Run one node on the connection of this thread."""
        return _run_node(
            name,
            dag,
            pool.acquire(),
            run_mode,
            run_id,
            after_node_success,
            use_staging,
        )

    try:
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="clair-run") as executor:
            running: dict[Future[_NodeOutcome], str] = {}

            while ready or running:
                # Report each node that clair must skip, and start the others.
                while ready and len(running) < threads:
                    name = ready.popleft()

                    if name in skip_reasons:
                        logical_address = _logical_address_of(dag, name)
                        logger.info("run.node.skipped", logical=logical_address, physical=name, skipped_by=skip_reasons[name])
                        yield RunResult(
                            logical_address=logical_address,
                            physical_address=name,
                            status=RunStatus.SKIPPED,
                            skipped_by=skip_reasons[name],
                        )
                        release(name)
                        continue

                    running[executor.submit(run_one, name)] = name

                # The loop above can report only skipped nodes, and start
                # nothing. The outer condition then ends the run. wait() on an
                # empty set would give an endless loop.
                if not running:
                    continue

                done, _ = wait(running, return_when=FIRST_COMPLETED)

                for future in done:
                    name = running.pop(future)
                    try:
                        outcome = future.result()
                    except Exception as node_error:  # noqa: BLE001 — one node that breaks must not stop the other branches
                        logger.warning("run.node.failure", physical=name, error=str(node_error))
                        outcome = _NodeOutcome(
                            RunResult(
                                logical_address=_logical_address_of(dag, name),
                                physical_address=name,
                                status=RunStatus.FAILURE,
                                error=str(node_error),
                            ),
                            downstream_ok=False,
                        )

                    if outcome.result is not None:
                        yield outcome.result

                    if not outcome.downstream_ok:
                        for descendant in downstream_nodes[name]:
                            skip_reasons.setdefault(descendant, name)

                    release(name)
    finally:
        pool.close()


def format_run_output(results: list[RunResult], env_name: str) -> RunSummary:
    """Make a RunSummary from the run results.

    Args:
        results: A list of RunResult objects.
        env_name: The name of the active environment.

    Returns:
        A RunSummary. It holds the data and supplies a .render() method.
    """
    return RunSummary(results=results, env_name=env_name)
