"""The runner. It executes the DAG on Snowflake in dependency order."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, NamedTuple

import networkx as nx
import pandas as pd
import structlog
from pydantic import BaseModel, Field

from clair.adapters.base import WarehouseAdapter
from clair.adapters.objects import Statement, StatementStatus, WarehouseColumn
from clair.adapters.pool import AdapterPool
from clair.core.dag import ClairDag, addresses_of, get_executable_nodes
from clair.core.schema_change import read_incremental_columns
from clair.core.staging import make_staging_address
from clair.exceptions import ClairError, ResultNotFoundError, RunError
from clair.trouves.address import NodeAddresses, TrouveAddress
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.plan import PlanContext, TestQuery, TestResult
from clair.trouves.run_config import Incremental, RunMode
from clair.trouves.statements import WarehouseComments, type_change_error
from clair.trouves.trouve import MaterializedTrouve, Table
from clair.trouves.trouve_type import TrouveType


class RunStatus(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class _RunResultBase(BaseModel):
    """The field that each result holds.

    Attributes:
        addresses: The logical address, the physical address, and the staging
            address of the node.
    """

    addresses: NodeAddresses


class RunSuccess(_RunResultBase):
    """The result of a Trouve that clair materialized at its physical address.

    Attributes:
        statements: Each statement of the node, in the order that clair made
            them. Each one holds its text, its query ID, its URL and its status.
        effective_run_mode: The run mode that clair used, after the RunConfig of
            the Trouve and the fallback to a full refresh.
        duration_seconds: The clock time of the statements.
        row_count: The number of rows that the last build statement changed.
        test_results: The data quality test results of this Trouve. A test that
            fails after a run with no staging does not change the status,
            because the physical object holds the new data.
    """

    status: Literal[RunStatus.SUCCESS] = RunStatus.SUCCESS
    statements: list[Statement]
    effective_run_mode: RunMode
    duration_seconds: float
    row_count: int
    test_results: list[TestResult] = []


class RunFailure(_RunResultBase):
    """The result of a Trouve that clair did not materialize at its physical address.

    A failure before the first statement holds the error only: for example a
    session context that clair cannot set, or an object of a different type at
    the address.

    Attributes:
        error: The cause of the failure. It is never empty.
        statements: Each statement of the node, in the order that clair made
            them. A statement after the one that failed has the NOT_RUN status.
        effective_run_mode: The run mode that clair used, or None if the node
            failed before clair selected it.
        duration_seconds: The clock time of the statements.
        test_results: The data quality test results of the staging object. A
            test that fails stops the promotion, and thus causes the failure.
    """

    status: Literal[RunStatus.FAILURE] = RunStatus.FAILURE
    error: str = Field(min_length=1)
    statements: list[Statement] = []
    effective_run_mode: RunMode | None = None
    duration_seconds: float = 0.0
    test_results: list[TestResult] = []

    @property
    def failed_statement(self) -> Statement | None:
        """Give the statement that failed, or None."""
        for statement in self.statements:
            if statement.status == StatementStatus.FAILURE:
                return statement
        return None


class RunSkip(_RunResultBase):
    """The result of a Trouve that clair did not run, because an upstream Trouve failed.

    Attributes:
        skipped_by: The physical address of the upstream Trouve that failed.
    """

    status: Literal[RunStatus.SKIPPED] = RunStatus.SKIPPED
    skipped_by: str


class RunCancel(_RunResultBase):
    """The result of a Trouve that clair did not start, because the run was cancelled.

    The physical object keeps its data. A retry selects each result whose
    status is not SUCCESS, thus it selects this Trouve too.
    """

    status: Literal[RunStatus.CANCELLED] = RunStatus.CANCELLED


# The result of one Trouve. The status field tells the classes apart, thus a
# pydantic model that holds a result reads it back from JSON.
RunResult = RunSuccess | RunFailure | RunSkip | RunCancel

# The error of a statement that clair did not send, because the run was cancelled.
RUN_CANCELLED_ERROR = "the run was cancelled, thus clair did not send this statement"


class RunCancellation:
    """The request to stop a run, from a thread that does not run it.

    A signal handler, or a caller of ``RunHandle.cancel()``, calls ``cancel()``.
    The runner then starts no new Trouve, sends no new statement, and promotes
    nothing. Each connection of the run also cancels the statement that it runs
    now, thus a long build stops in the warehouse too.
    """

    def __init__(self) -> None:
        self._cancelled = threading.Event()
        self._lock = threading.Lock()
        self._adapters: list[WarehouseAdapter] = []

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    def watch(self, adapter: WarehouseAdapter) -> None:
        """Cancel the statements of *adapter* when the run is cancelled."""
        with self._lock:
            self._adapters.append(adapter)
            cancelled = self._cancelled.is_set()
        if cancelled:
            _cancel_statement_of(adapter)

    def cancel(self) -> None:
        """Stop the run. A second call does nothing."""
        with self._lock:
            if self._cancelled.is_set():
                return
            self._cancelled.set()
            adapters = list(self._adapters)
        logger.warning("run.cancel", connections=len(adapters))
        for adapter in adapters:
            _cancel_statement_of(adapter)


def _cancel_statement_of(adapter: WarehouseAdapter) -> None:
    """Ask one connection to cancel its statement. A fault here must not stop the cancel."""
    try:
        adapter.cancel()
    except Exception as cancel_error:  # noqa: BLE001 — the runner still sends nothing more
        logger.warning("run.cancel_error", error=str(cancel_error))


def _render_statements(statements: list[Statement]) -> list[str]:
    """Make a query ID line and a URL line for each statement that has an ID.

    Add an [i/n] label if the node has 2 statements or more.
    """
    lines: list[str] = []
    identified = [statement for statement in statements if statement.query_id]
    total = len(identified)
    for index, statement in enumerate(identified, 1):
        label = f" [{index}/{total}]" if total > 1 else ""
        lines.append(f"      Query ID{label}: {statement.query_id}")
        if statement.query_url:
            lines.append(f"      URL{label}: {statement.query_url}")
    return lines


class RunSummary(BaseModel):
    """The result of one run operation."""

    results: list[Annotated[RunResult, Field(discriminator="status")]]
    env_name: str
    run_id: str = ""
    project_root: Path | None = None
    run_mode: RunMode = RunMode.FULL_REFRESH
    # The row limit of a sampled run. None tells that the run read each row.
    sample_rows: int | None = None

    def result(self, address: str) -> RunResult:
        """Find one result by its logical address or its physical address.

        An address that no result holds is a fault of the caller, thus this
        method raises. A caller that examines an optional address reads
        `results` directly.
        """
        for run_result in self.results:
            if run_result.addresses.matches(address):
                return run_result
        known_addresses = ", ".join(
            str(run_result.addresses.logical) for run_result in self.results
        )
        raise ResultNotFoundError(
            f"The run holds no result for '{address}'. "
            f"The run holds these addresses: {known_addresses or '(none)'}"
        )

    def with_status(self, status: RunStatus) -> list[RunResult]:
        """Give each result with this status, in the run order."""
        return [r for r in self.results if r.status == status]

    @property
    def test_results(self) -> list[TestResult]:
        """Each data quality test result of the run, in the run order."""
        return [
            test_result
            for run_result in self.results
            if isinstance(run_result, RunSuccess | RunFailure)
            for test_result in run_result.test_results
        ]

    @property
    def succeeded(self) -> list[RunSuccess]:
        return [r for r in self.results if isinstance(r, RunSuccess)]

    @property
    def failed(self) -> list[RunFailure]:
        return [r for r in self.results if isinstance(r, RunFailure)]

    @property
    def skipped(self) -> list[RunSkip]:
        return [r for r in self.results if isinstance(r, RunSkip)]

    @property
    def cancelled(self) -> list[RunCancel]:
        return [r for r in self.results if isinstance(r, RunCancel)]

    @property
    def cancelled_count(self) -> int:
        return len(self.cancelled)

    @property
    def succeeded_count(self) -> int:
        return len(self.succeeded)

    @property
    def failed_count(self) -> int:
        return len(self.failed)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)

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
        name = str(result.addresses.physical)

        if isinstance(result, RunSkip):
            lines.append(f"[{index}/{total}] {name} ... SKIPPED")
            lines.append(f"      Reason: the upstream dependency {result.skipped_by} failed")
        elif isinstance(result, RunCancel):
            lines.append(f"[{index}/{total}] {name} ... CANCELLED")
            lines.append("      Reason: the run was cancelled before this Trouve started")
        elif isinstance(result, RunSuccess):
            lines.append(
                f"[{index}/{total}] {name} ... OK ({result.duration_seconds:.1f}s)"
            )
            lines.extend(_render_statements(result.statements))
        else:
            lines.append(
                f"[{index}/{total}] {name} ... FAILED ({result.duration_seconds:.1f}s)"
            )
            lines.extend(_render_statements(result.statements))
            lines.append(f"      Error: {result.error}")
            if result.statements:
                lines.append("      SQL:")
                for statement in result.statements:
                    for sql_line in statement.sql.strip().splitlines():
                        lines.append(f"        {sql_line}")
                    lines.append("")

        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def render_footer(succeeded: int, failed: int, skipped: int, cancelled: int = 0) -> str:
        """Make the last line of the summary."""
        cancelled_text = f", {cancelled} cancelled" if cancelled else ""
        return f"=== Done: {succeeded} succeeded, {failed} failed, {skipped} skipped{cancelled_text} ==="

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        total = len(self.results)
        parts = [self.render_header(total, self.env_name)]

        for i, r in enumerate(self.results, 1):
            parts.append(self.render_node(r, i, total))

        parts.append(self.render_footer(
            self.succeeded_count, self.failed_count, self.skipped_count, self.cancelled_count,
        ))

        return "\n".join(parts)


logger = structlog.get_logger()


def _write_dataframe(
    trouve: DataframeTrouve, adapter: WarehouseAdapter, address: TrouveAddress
) -> tuple[Statement | None, str]:
    """Read the inputs of a DataframeTrouve, build its DataFrame, and write it to *address*.

    The write takes the place of SQL statements in the build phase. The plan
    statements of the build phase run after it.

    Returns:
        The statement of the write, and an empty error. Or None, and the error.
    """
    # 1. Read each input DataFrame. Clair keeps the order of the inputs.
    # compiled.inputs holds the read of each input, in the order of the
    # inputs. recompile_for_selection() sets it: the physical address of an
    # input that this run builds, the logical production address of an input
    # that it does not build, and the filter of a sampled run.
    input_dataframes: list[pd.DataFrame] = []
    compiled = trouve.require_compiled()
    assert len(compiled.inputs) == len(trouve.upstream_trouves())
    for parameter_name, dataframe_input in zip(trouve.parameter_names(), compiled.inputs):
        try:
            input_dataframes.append(
                adapter.fetch_dataframe(
                    TrouveAddress.parse(dataframe_input.address),
                    sample_query=dataframe_input.sample_query,
                )
            )
        except Exception as fetch_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
            return None, (
                f"Clair cannot read the input '{parameter_name}' ({dataframe_input.address}): {fetch_error}"
            )

    # 2. Build the DataFrame. Clair binds each input by position.
    try:
        result_dataframe = trouve.build_dataframe(*input_dataframes)
    except Exception as transform_error:  # noqa: BLE001 — the user code is unknown
        return None, f"Clair cannot build the DataFrame: {transform_error}"

    if not isinstance(result_dataframe, pd.DataFrame):
        return None, (
            f"The Trouve must give a pandas DataFrame, "
            f"but it gave {type(result_dataframe).__name__}"
        )

    # 3. Write the result to Snowflake.
    try:
        statement = adapter.write_dataframe(
            dataframe=result_dataframe, address=address, transient=trouve.is_transient
        )
    except Exception as write_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
        return None, f"Clair cannot write the DataFrame to {address}: {write_error}"
    if not statement.success:
        return None, statement.error or f"Clair cannot write the DataFrame to {address}"
    return statement, ""


def _cancelled_before_promotion(addresses: NodeAddresses) -> str:
    """Give the error of a node that built its candidate, and stopped before the promotion."""
    return (
        f"the run was cancelled before the promotion. {addresses.physical} keeps its "
        f"data. The candidate stays at {addresses.staging}"
    )


def _rejection_error(
    trouve: MaterializedTrouve,
    context: PlanContext,
    test_results: list[TestResult],
) -> str:
    """Give the error of a node whose should_promote() gave False."""
    staging_address = context.staging_address
    if any(test_result.is_blocking for test_result in test_results):
        return (
            f"the tests failed. {context.physical_address} keeps its data. "
            f"The rejected candidate stays at {staging_address}"
        )
    type_error = trouve.type_change_error(context)
    if type_error is not None:
        return (
            f"the tests passed, but clair does not promote: {type_error}. "
            f"The tested candidate stays at {staging_address}"
        )
    return (
        f"should_promote() gave False. {context.physical_address} keeps its data. "
        f"The candidate stays at {staging_address}"
    )


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


# The callback that executes the test queries of one node, on the connection of
# the thread, and gives the results.
TestNode = Callable[[str, list[TestQuery], WarehouseAdapter], list[TestResult]]


def _run_node(
    name: str,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    run_mode: RunMode,
    run_id: str,
    test_node: TestNode | None,
    use_staging: bool,
    environment_name: str,
    cancellation: RunCancellation | None = None,
) -> _NodeOutcome:
    """Materialize one Trouve in the warehouse: read, plan, then execute each phase.

    A thread of the run calls this function. *adapter* is the private connection
    of that thread, and the function gives the same adapter to *test_node*.

    The function reads the warehouse, gives what it read to ``trouve.plan()``,
    and executes the phases of the plan in order. See ``clair.trouves.plan``.

    Args:
        name: The physical address of the node, which is the key in the DAG.
        dag: The project DAG.
        adapter: The warehouse adapter of the thread that calls this function.
        run_mode: The run mode that the command line gives.
        run_id: The identifier of the run. A staging address holds it.
        test_node: The callback that executes the test queries.
        use_staging: True if clair writes to a staging address first.
        environment_name: The environment of the run. The query tag holds it.
        cancellation: The request to stop the run. After it, the node sends no
            new statement and promotes nothing. The cleanup still runs.

    Returns:
        A _NodeOutcome. It holds the result, and it tells you if clair can
        continue with the nodes downstream.
    """
    # Each DAG node has the physical address as its key. The logs and the
    # results show both names, thus the reader sees the file that made the
    # Trouve, and the object that clair writes.
    addresses = addresses_of(dag, name)
    logical_address = str(addresses.logical)
    trouve = dag.get_trouve(name)
    assert trouve.compiled is not None
    assert isinstance(trouve, MaterializedTrouve), "clair never materializes a Source"

    def is_cancelled() -> bool:
        return cancellation is not None and cancellation.is_cancelled

    def failure(error: str, **result_fields: Any) -> _NodeOutcome:
        logger.warning("run.node.failure", logical=logical_address, physical=name, error=error)
        return _NodeOutcome(
            RunFailure(addresses=addresses, error=error, **result_fields), downstream_ok=False
        )

    if is_cancelled():
        return _NodeOutcome(RunCancel(addresses=addresses), downstream_ok=False)

    config = trouve.compiled.config
    try:
        adapter.set_session_parameters(
            query_tag=config.query_tag(run_id, environment_name, logical_address),
            statement_timeout_seconds=config.statement_timeout_seconds,
        )
        adapter.set_context(warehouse=config.warehouse, role=config.role)
    except Exception as e:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
        logger.warning("run.node.context_error", logical=logical_address, physical=name, warehouse=config.warehouse, role=config.role, error=str(e))
        return _NodeOutcome(
            RunFailure(addresses=addresses, error=f"Clair cannot set the session context: {e}"),
            downstream_ok=False,
        )

    # A staged run materializes the Trouve at a run-scoped address beside the
    # physical one. Clair writes the physical address only after the tests on
    # that object pass.
    if use_staging:
        try:
            addresses = addresses_of(dag, name, make_staging_address(addresses.physical, run_id))
        except ClairError as naming_error:
            return failure(str(naming_error))
    staging_address = addresses.staging

    # ── read: what the plan needs to know ─────────────────────────────────
    # One read of the physical object for each node. It gives the type for the
    # type test and for the run mode, and the comment of the object.
    try:
        current_object = adapter.describe_object(addresses.physical)
    except ClairError as describe_error:
        return failure(str(describe_error))

    # A run with no staging writes the physical address directly, thus it has no
    # candidate to keep. A Trouve that changed its type stops before any write.
    # A staged run builds and tests first, and should_promote() stops it.
    if staging_address is None:
        type_error = type_change_error(
            current_object, trouve.warehouse_object_type, addresses.physical
        )
        if type_error is not None:
            return failure(type_error)

    effective_mode = trouve.effective_mode(run_mode, current_object)
    if run_mode == RunMode.INCREMENTAL and effective_mode == RunMode.FULL_REFRESH and isinstance(trouve, Table) and isinstance(trouve.run_config, Incremental):
        if current_object is None:
            reason = "object_not_found"
        elif current_object.object_type != trouve.warehouse_object_type:
            reason = "object_type_changed"
        else:
            reason = "effective_mode"
        logger.info("run.node.incremental_fallback", logical=logical_address, physical=name, reason=reason)

    # The INSERT names each column of the target, and the SQL must give the
    # same columns. read_incremental_columns() compares the two lists.
    target_columns = None
    added_columns: list[WarehouseColumn] = []
    if effective_mode == RunMode.INCREMENTAL and isinstance(trouve, Table):
        try:
            incremental_columns = read_incremental_columns(trouve, adapter, addresses.physical)
        except ClairError as column_error:
            return failure(str(column_error))
        target_columns = incremental_columns.target_columns
        added_columns = incremental_columns.change.added_columns
        if added_columns:
            logger.info("run.node.schema_change", logical=logical_address, physical=name, added_columns=[column.name for column in added_columns])

    # An incremental table keeps its comments, and the two reads above hold
    # them. The default document() writes only a comment that differs.
    current_comments = None
    if target_columns is not None and current_object is not None:
        current_comments = WarehouseComments.of(current_object, target_columns)

    context = PlanContext(
        run_id=run_id,
        effective_mode=effective_mode,
        staged=staging_address is not None,
        physical_address=addresses.physical,
        staging_address=staging_address,
        current_object=current_object,
        target_columns=target_columns,
        added_columns=added_columns,
        current_comments=current_comments,
    )

    # ── plan ──────────────────────────────────────────────────────────────
    try:
        plan = trouve.plan(context)
    except Exception as plan_error:  # noqa: BLE001 — plan() can be user code, and it can raise anything
        return failure(f"{type(trouve).__name__}.plan() raised {type(plan_error).__name__}: {plan_error}")

    is_dataframe = isinstance(trouve, DataframeTrouve)
    # A SQL Trouve with no build statement makes no object, thus clair has
    # nothing to execute and nothing to tell.
    if not is_dataframe and not plan.build:
        return _NodeOutcome(None, downstream_ok=True)

    # ── execute ───────────────────────────────────────────────────────────
    logger.info("run.node.start", logical=logical_address, physical=name, effective_mode=effective_mode.value)
    start = time.monotonic()
    statements: list[Statement] = []

    def execute(sql: str) -> Statement:
        """Send one statement, or record it as cancelled after a cancel."""
        if is_cancelled():
            statement = Statement(sql=sql, status=StatementStatus.FAILURE, error=RUN_CANCELLED_ERROR)
        else:
            statement = adapter.execute(sql)
        statements.append(statement)
        return statement

    def execute_phase(phase_statements: list[str]) -> Statement | None:
        """Execute each statement of one phase. Give the statement that failed, or None."""
        for sql in phase_statements:
            statement = execute(sql)
            if not statement.success:
                return statement
        return None

    def run_cleanup() -> None:
        """Execute the cleanup phase. It runs last, after a failure and after a cancel too."""
        for sql in plan.cleanup:
            cleanup_statement = adapter.execute(sql)
            statements.append(cleanup_statement)
            if not cleanup_statement.success:
                logger.warning("run.node.cleanup_failure", logical=logical_address, physical=name, error=cleanup_statement.error)

    def finish(error: str, downstream_ok: bool, row_count: int = 0) -> _NodeOutcome:
        """Run the cleanup, and give the result of the node."""
        run_cleanup()
        duration = time.monotonic() - start
        query_ids = [statement.query_id for statement in statements if statement.query_id]
        if error:
            logger.warning("run.node.failure", logical=logical_address, physical=name, duration_seconds=round(duration, 3), error=error, query_ids=query_ids)
            return _NodeOutcome(
                RunFailure(addresses=addresses, statements=statements, effective_run_mode=effective_mode, error=error, duration_seconds=duration),
                downstream_ok=False,
            )
        logger.info("run.node.success", logical=logical_address, physical=name, duration_seconds=round(duration, 3), query_ids=query_ids)
        return _NodeOutcome(
            RunSuccess(addresses=addresses, statements=statements, effective_run_mode=effective_mode, duration_seconds=duration, row_count=row_count),
            downstream_ok=downstream_ok,
        )

    # prepare, build and configure write the target. clair stops at the first
    # statement that fails, and records each statement after it as not run,
    # thus the reader sees the complete plan and the point where clair stopped.
    staging_note = (
        "" if staging_address is None
        else f". Clair keeps the staging object at {staging_address}, if the {'write' if is_dataframe else 'build'} made one"
    )
    sent_write_count = 0
    row_count = 0

    def stop_writes(error: str) -> _NodeOutcome:
        """Record each write statement that clair did not send, and give the failure."""
        statements.extend(Statement(sql=sql) for sql in plan.write_statements[sent_write_count:])
        return finish(f"{error}{staging_note}", downstream_ok=False)

    failed_statement = execute_phase(plan.prepare)
    sent_write_count = len(statements)
    if failed_statement is not None:
        return stop_writes(failed_statement.error or "unknown error")

    if isinstance(trouve, DataframeTrouve):
        write_statement, write_error = (
            (None, RUN_CANCELLED_ERROR) if is_cancelled()
            else _write_dataframe(trouve, adapter, context.target_address)
        )
        if write_statement is None:
            return stop_writes(write_error)
        statements.append(write_statement)
        row_count = write_statement.row_count

    build_start = len(statements)
    failed_statement = execute_phase(plan.build)
    sent_write_count += len(statements) - build_start
    if failed_statement is not None:
        return stop_writes(failed_statement.error or "unknown error")
    if plan.build:
        # The last statement that writes rows gives the count.
        row_count = statements[-1].row_count

    configure_start = len(statements)
    failed_statement = execute_phase(plan.configure)
    sent_write_count += len(statements) - configure_start
    if failed_statement is not None:
        return stop_writes(failed_statement.error or "unknown error")

    # A run with no staging wrote production. The comments follow the build,
    # and the tests report on the written object. Nothing waits on them.
    if staging_address is None:
        failed_statement = execute_phase(plan.document)
        if failed_statement is not None:
            return finish(failed_statement.error or "unknown error", downstream_ok=False)
        downstream_ok = True
        if test_node is not None:
            test_results = test_node(name, plan.test, adapter)
            downstream_ok = not any(test_result.is_blocking for test_result in test_results)
        return finish("", downstream_ok=downstream_ok, row_count=row_count)

    # A staged run tests the candidate, then promotes it or keeps it. A cancel
    # can arrive while the tests run, thus clair asks again after them, just
    # before the promotion.
    assert test_node is not None
    test_results = [] if is_cancelled() else test_node(name, plan.test, adapter)
    if is_cancelled():
        return finish(_cancelled_before_promotion(addresses), downstream_ok=False)
    if not trouve.should_promote(context, test_results):
        return finish(_rejection_error(trouve, context, test_results), downstream_ok=False)

    for statement_index, sql in enumerate([*plan.promote, *plan.document]):
        statement = execute(sql)
        if statement.success:
            continue
        error = statement.error or "unknown error"
        if statement_index == 0:
            return finish(
                f"the tests passed, but the promotion failed: {error}. "
                f"The candidate stays at {staging_address}",
                downstream_ok=False,
            )
        # The first statement of the promotion succeeded, thus the physical
        # address holds the tested data. The object is not yet in the state that
        # the Trouve declares.
        return finish(
            f"{addresses.physical} holds the tested data, but a statement after "
            f"the promotion failed: {error}. Run it again: {sql.splitlines()[-1]}",
            downstream_ok=False,
        )

    # The physical address now holds the tested data. A fault in the retire
    # phase makes clutter, not a wrong table, thus the node keeps the SUCCESS
    # status.
    for sql in plan.retire:
        retire_statement = adapter.execute(sql)
        statements.append(retire_statement)
        if not retire_statement.success:
            logger.warning("run.node.staging_drop_failed", trouve=str(addresses.physical), staging=str(staging_address), error=retire_statement.error)

    return finish("", downstream_ok=True, row_count=row_count)


def run_project(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    test_node: TestNode | None = None,
    use_staging: bool = False,
    threads: int = 1,
    environment_name: str = "",
    cancellation: RunCancellation | None = None,
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
        test_node: The callback that executes the test queries of one node, and
            gives the results. The three arguments are the node name, the
            queries of the test phase of the plan, and the connection of the
            thread. In a staged run, the results go to should_promote(). A
            blocking result skips each node downstream.
        use_staging: if True, clair writes each node to a run-scoped staging
            address, runs the tests there, and promotes the object only after
            the tests pass. The tests decide the promotion, so this needs
            test_node.
        threads: The number of Trouves that clair runs at one time, and thus the
            number of warehouse connections.
        cancellation: The request to stop the run. After it, clair starts no
            new Trouve and gives a RunCancel for each one, and each Trouve that
            runs stops before its next statement and before its promotion.

    Raises:
        RunError: If *use_staging* is True and *test_node* is None.
        ValueError: If *threads* is less than 1.
    """
    if use_staging and test_node is None:
        raise RunError(
            "A staged run needs the tests. Give test_node, because the "
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
    if cancellation is not None:
        for pooled_adapter in pool.adapters:
            cancellation.watch(pooled_adapter)

    def run_one(name: str) -> _NodeOutcome:
        """Run one node on the connection of this thread."""
        return _run_node(
            name,
            dag,
            pool.acquire(),
            run_mode,
            run_id,
            test_node,
            use_staging,
            environment_name,
            cancellation,
        )

    try:
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="clair-run") as executor:
            running: dict[Future[_NodeOutcome], str] = {}

            while ready or running:
                # Report each node that clair must skip, and start the others.
                while ready and len(running) < threads:
                    name = ready.popleft()

                    if name in skip_reasons:
                        addresses = addresses_of(dag, name)
                        logger.info("run.node.skipped", logical=str(addresses.logical), physical=name, skipped_by=skip_reasons[name])
                        yield RunSkip(
                            addresses=addresses,
                            skipped_by=skip_reasons[name],
                        )
                        release(name)
                        continue

                    if cancellation is not None and cancellation.is_cancelled:
                        yield RunCancel(addresses=addresses_of(dag, name))
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
                            RunFailure(
                                addresses=addresses_of(dag, name),
                                # An exception can hold an empty message, and a
                                # failure needs a cause.
                                error=str(node_error) or type(node_error).__name__,
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
