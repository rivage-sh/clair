"""The runner. It executes the DAG on Snowflake in dependency order."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, NamedTuple

import networkx as nx
import pandas as pd
import structlog
from pydantic import BaseModel, Field

from clair.adapters.base import (
    Statement,
    StatementStatus,
    WarehouseAdapter,
    WarehouseObject,
    WarehouseObjectType,
)
from clair.adapters.pool import AdapterPool
from clair.core.comments import WarehouseComments, build_comment_statements
from clair.core.dag import ClairDag, addresses_of, get_executable_nodes
from clair.core.schema_change import read_incremental_columns
from clair.core.staging import (
    build_clone_statement,
    build_cluster_by_statements,
    build_drop_staging_statement,
    build_promote_statements,
    make_staging_address,
)
from clair.core.test_runner import TestResult
from clair.exceptions import ClairError, ResultNotFoundError, RunError
from clair.trouves.address import NodeAddresses, TrouveAddress
from clair.trouves.dataframe_trouve import DataframeTrouve
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import ExecutionType, SqlTrouve, Table, TrouveAbc, table_options_of
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


def resolve_effective_mode(trouve: TrouveAbc, cli_run_mode: RunMode) -> RunMode:
    """Find the effective run mode of a Trouve. This function needs no adapter.

    The compiler and the runner both call it. The compiler has no connection.
    The runner also asks the warehouse if the table exists, before it accepts
    the incremental mode.
    """
    # Only a Table holds a run_config. A View, a Source and a DataFrame Trouve
    # always make the object again.
    if not isinstance(trouve, Table) or trouve.run_config.run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    if cli_run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    return RunMode.INCREMENTAL


def _run_dataframe_trouve(
    trouve: DataframeTrouve,
    adapter: WarehouseAdapter,
    addresses: NodeAddresses,
    effective_run_mode: RunMode,
) -> RunSuccess | RunFailure:
    """Execute a DataframeTrouve. Read the inputs, build the DataFrame, write it.

    Clair writes the DataFrame to ``addresses.target``: the staging address of a
    staged run, or the physical address.

    Args:
        trouve: The Trouve to execute.
        adapter: A warehouse adapter with an open connection.
        addresses: The addresses of the node.
        effective_run_mode: The run mode of the result.

    Returns a RunSuccess or a RunFailure.
    """
    start = time.monotonic()

    def failure(message: str) -> RunFailure:
        """Make the result of a DataframeTrouve that did not write its table."""
        return RunFailure(
            addresses=addresses,
            effective_run_mode=effective_run_mode,
            error=message,
            duration_seconds=time.monotonic() - start,
        )

    # 1. Read each input DataFrame. Clair keeps the order of the inputs.
    input_dataframes: list[pd.DataFrame] = []
    # compiled.inputs holds the read of each input, in the order of the
    # inputs. recompile_for_selection() sets it: the physical address of an
    # input that this run builds, the logical production address of an input
    # that it does not build, and the filter of a sampled run.
    assert trouve.compiled is not None
    assert len(trouve.compiled.inputs) == len(trouve.upstream_trouves())
    for parameter_name, dataframe_input in zip(
        trouve.parameter_names(), trouve.compiled.inputs
    ):
        try:
            input_dataframes.append(
                adapter.fetch_dataframe(
                    TrouveAddress.parse(dataframe_input.address),
                    sample_query=dataframe_input.sample_query,
                )
            )
        except Exception as fetch_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
            return failure(
                f"Clair cannot read the input '{parameter_name}' ({dataframe_input.address}): {fetch_error}"
            )

    # 2. Build the DataFrame. Clair binds each input by position.
    try:
        result_dataframe = trouve.build_dataframe(*input_dataframes)
    except Exception as transform_error:  # noqa: BLE001 — the user code is unknown
        return failure(f"Clair cannot build the DataFrame: {transform_error}")

    # 3. Make sure that the result is a DataFrame.
    if not isinstance(result_dataframe, pd.DataFrame):
        return failure(
            f"The Trouve must give a pandas DataFrame, "
            f"but it gave {type(result_dataframe).__name__}"
        )

    # 4. Write the result to Snowflake. A TrouveAddress is valid when it exists,
    # so the three names need no test here.
    address = addresses.target

    try:
        statement = adapter.write_dataframe(
            dataframe=result_dataframe, address=address, transient=trouve.is_transient
        )
    except Exception as write_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
        return failure(f"Clair cannot write the DataFrame to {address}: {write_error}")

    if not statement.success:
        return failure(statement.error or f"Clair cannot write the DataFrame to {address}")

    # A staged run writes the comments after the promotion, in _promote_or_keep.
    after_write_statements = build_cluster_by_statements(address, trouve)
    if addresses.staging is None:
        after_write_statements.extend(build_comment_statements(address, trouve))
    statements = [statement]
    for sql in after_write_statements:
        statements.append(adapter.execute(sql))
        if not statements[-1].success:
            return failure(statements[-1].error or f"Clair cannot complete {address}")

    return RunSuccess(
        addresses=addresses,
        statements=statements,
        effective_run_mode=effective_run_mode,
        duration_seconds=time.monotonic() - start,
        row_count=statement.row_count,
    )


def wanted_object_type(trouve: TrouveAbc) -> WarehouseObjectType:
    """Give the warehouse object type that the Trouve must have at its address.

    Clair never materializes a SOURCE Trouve, thus a SOURCE never reaches this
    function.
    """
    assert trouve.type != TrouveType.SOURCE, "clair does not materialize a SOURCE Trouve"
    if trouve.type == TrouveType.VIEW:
        return WarehouseObjectType.VIEW
    table_options = table_options_of(trouve)
    if table_options is not None and table_options.iceberg_settings() is not None:
        return WarehouseObjectType.ICEBERG_TABLE
    return WarehouseObjectType.TABLE


def type_change_error(
    trouve: TrouveAbc,
    current_object: WarehouseObject | None,
    physical_address: TrouveAddress,
) -> str | None:
    """Give the error that stops a Trouve whose type differs from the object at its address.

    A warehouse replaces an object with an object of the same type only. A
    Trouve that changes between a TABLE, an ICEBERG TABLE and a VIEW thus needs
    a drop of the old object. Clair does not send that drop. A drop removes
    each privilege on the object, and the role of the run often cannot grant
    them again, for example in a managed access schema. The person who changed
    the type must drop the object, and decide about the grants.

    The runner asks the warehouse for the type one time for each node, because
    the compiler opens no connection and cannot know it.

    Args:
        trouve: The Trouve as it is now.
        current_object: The object at the physical address, as the runner read
            it at the start of the node. None means that the address holds nothing.
        physical_address: The address that clair writes to.

    Returns:
        The error message, or None when the address holds nothing or an object
        of the correct type. The caller must stop before it writes the physical
        address.
    """
    current_type = None if current_object is None else current_object.object_type
    new_type = wanted_object_type(trouve)
    if current_type is None or current_type == new_type:
        return None

    old_type_name = current_type.value.upper()
    new_type_name = new_type.value.upper()
    return (
        f"{physical_address} holds the type {old_type_name}, and the Trouve now "
        f"makes the type {new_type_name}. Snowflake cannot replace one type with the other. "
        f"Clair does not drop the {old_type_name}, because a drop removes each "
        f"grant on it. Run DROP {old_type_name} {physical_address}, then run "
        f"clair again"
    )


def _promote_or_keep(
    trouve: SqlTrouve | DataframeTrouve,
    adapter: WarehouseAdapter,
    addresses: NodeAddresses,
    tests_passed: bool,
    current_object: WarehouseObject | None,
    current_comments: WarehouseComments | None = None,
) -> tuple[list[Statement], str]:
    """Complete a node: promote the staging object, or keep it for an examination.

    Args:
        trouve: The Trouve that clair materialized at the staging address.
        adapter: A warehouse adapter with an open connection.
        addresses: The addresses of the node. The staging address holds the
            candidate data, and the physical address must receive it.
        tests_passed: The result of the data quality tests on the staging object.
        current_object: The object at the physical address at the start of the
            node, or None. The promotion stops if its type differs.
        current_comments: The comments of the physical table before the run.
            None means a new object. The promotion of an incremental table
            clones a clone of the physical table, thus it keeps them.

    Returns:
        The statements that clair executed, and an error message. The error
        message is empty when the physical address holds the tested data.
    """
    staging_address = addresses.staging
    assert staging_address is not None
    physical_address = addresses.physical
    statements: list[Statement] = []

    if not tests_passed:
        return statements, (
            f"the tests failed. {physical_address} keeps its data. "
            f"The rejected candidate stays at {staging_address}"
        )

    assert trouve.compiled is not None

    # The tests passed, so the candidate must reach the physical address. A
    # Trouve that changed its type finds an object of the old type there. Clair
    # stops, and keeps the tested candidate for the person who fixes the address.
    type_error = type_change_error(trouve, current_object, physical_address)
    if type_error is not None:
        return statements, (
            f"the tests passed, but clair does not promote: {type_error}. "
            f"The tested candidate stays at {staging_address}"
        )

    # The comments go on after the promotion: a promotion of a view removes each
    # comment that the staging view had.
    promotion_statements = [
        *build_promote_statements(
            trouve.type,
            staging_address=staging_address,
            physical_address=physical_address,
            resolved_sql=trouve.compiled.resolved_sql if isinstance(trouve, SqlTrouve) else "",
            table_options=table_options_of(trouve),
        ),
        *build_comment_statements(physical_address, trouve, current_comments),
    ]
    for statement_index, sql in enumerate(promotion_statements):
        statements.append(adapter.execute(sql))
        if statements[-1].success:
            continue
        error = statements[-1].error or "unknown error"
        if statement_index == 0:
            return statements, (
                f"the tests passed, but the promotion failed: {error}. "
                f"The candidate stays at {staging_address}"
            )
        # The clone succeeded, thus the physical address holds the tested data.
        # The table is not yet in the state that the Trouve declares.
        return statements, (
            f"{physical_address} holds the tested data, but a statement after "
            f"the promotion failed: {error}. Run it again: {sql.splitlines()[-1]}"
        )

    # The physical address now holds the tested data, so the staging copy has no
    # more use. A fault here makes clutter, not a wrong table, thus the node
    # keeps the SUCCESS status.
    drop = adapter.execute(build_drop_staging_statement(trouve.type, staging_address))
    statements.append(drop)
    if not drop.success:
        logger.warning(
            "run.node.staging_drop_failed",
            trouve=str(physical_address),
            staging=str(staging_address),
            error=drop.error,
        )

    return statements, ""


def _cancelled_before_promotion(addresses: NodeAddresses) -> str:
    """Give the error of a node that built its candidate, and stopped before the promotion."""
    return (
        f"the run was cancelled before the promotion. {addresses.physical} keeps its "
        f"data. The candidate stays at {addresses.staging}"
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


def _run_node(
    name: str,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    run_mode: RunMode,
    run_id: str,
    after_node_success: Callable[[str, str, WarehouseAdapter], bool] | None,
    use_staging: bool,
    environment_name: str,
    cancellation: RunCancellation | None = None,
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
        environment_name: The environment of the run. The query tag holds it.
        cancellation: The request to stop the run. After it, the node sends no
            new statement and promotes nothing.

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

    def is_cancelled() -> bool:
        return cancellation is not None and cancellation.is_cancelled

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
            RunFailure(
                addresses=addresses,
                error=f"Clair cannot set the session context: {e}",
            ),
            downstream_ok=False,
        )

    # A staged run materializes the Trouve at a run-scoped address beside the
    # physical one. Clair writes the physical address only after the tests on
    # that object pass.
    if use_staging:
        try:
            addresses = addresses_of(
                dag, name, make_staging_address(addresses.physical, run_id)
            )
        except ClairError as naming_error:
            logger.warning("run.node.failure", logical=logical_address, physical=name, error=str(naming_error))
            return _NodeOutcome(
                RunFailure(addresses=addresses, error=str(naming_error)),
                downstream_ok=False,
            )

    staging_address = addresses.staging

    # One read of the physical object for each node. It gives the type for the
    # type test and for the incremental mode, and the comment of the object.
    current_object = (
        None if trouve.type == TrouveType.SOURCE else adapter.describe_object(addresses.physical)
    )

    # A run with no staging writes the physical address directly, thus it has no
    # candidate to keep. A Trouve that changed its type stops before any write.
    # A staged run builds and tests first, and stops in _promote_or_keep.
    if staging_address is None and trouve.type != TrouveType.SOURCE:
        type_error = type_change_error(trouve, current_object, addresses.physical)
        if type_error is not None:
            logger.warning("run.node.failure", logical=logical_address, physical=name, error=type_error)
            return _NodeOutcome(
                RunFailure(addresses=addresses, error=type_error),
                downstream_ok=False,
            )

    # A DataframeTrouve is different. Clair reads the data, builds a DataFrame
    # and writes it. Clair does not execute SQL.
    if trouve.execution_type == ExecutionType.PANDAS:
        assert isinstance(trouve, DataframeTrouve)
        logger.info("run.node.start", logical=logical_address, physical=name, effective_mode="full_refresh")
        result = _run_dataframe_trouve(trouve, adapter, addresses, RunMode.FULL_REFRESH)

        downstream_ok = True

        if isinstance(result, RunSuccess) and staging_address is not None:
            assert after_node_success is not None
            # A cancel can arrive while the tests run, thus clair asks again
            # after them, just before the promotion.
            tests_passed = not is_cancelled() and after_node_success(name, str(staging_address), adapter)
            if is_cancelled():
                promote_statements, staging_error = [], _cancelled_before_promotion(addresses)
            else:
                promote_statements, staging_error = _promote_or_keep(
                    trouve, adapter, addresses, tests_passed, current_object
                )
            statements = result.statements + promote_statements
            if staging_error:
                result = RunFailure(
                    addresses=result.addresses,
                    error=staging_error,
                    statements=statements,
                    effective_run_mode=result.effective_run_mode,
                    duration_seconds=result.duration_seconds,
                )
            else:
                result = result.model_copy(update={"statements": statements})
        elif isinstance(result, RunFailure) and staging_address is not None:
            result = result.model_copy(
                update={
                    "error": (
                        f"{result.error}. Clair keeps the staging object at "
                        f"{staging_address}, if the write made one"
                    )
                }
            )

        if isinstance(result, RunSuccess):
            logger.info("run.node.success", logical=logical_address, physical=name, duration_seconds=round(result.duration_seconds, 3))
            # Without staging the tests run after clair wrote the physical object.
            if staging_address is None and after_node_success is not None:
                downstream_ok = after_node_success(name, str(addresses.physical), adapter)
        else:
            logger.warning("run.node.failure", logical=logical_address, physical=name, duration_seconds=round(result.duration_seconds, 3), error=result.error)
            downstream_ok = False

        return _NodeOutcome(result, downstream_ok)

    assert isinstance(trouve, SqlTrouve)
    effective_mode = resolve_effective_mode(trouve, run_mode)
    # An incremental run adds to the rows that the target table holds, thus it
    # needs a target table. Two conditions give it none: the address holds
    # nothing, and the address holds a view because the Trouve was a VIEW
    # before. Both change the run to the full refresh mode. For the view, the
    # full refresh builds and tests a candidate, and the promotion then stops,
    # because the address holds an object of the old type.
    if effective_mode == RunMode.INCREMENTAL:
        current_type = None if current_object is None else current_object.object_type
        if current_type is None:
            logger.info("run.node.incremental_fallback", logical=logical_address, physical=name, reason="object_not_found")
            effective_mode = RunMode.FULL_REFRESH
        elif current_type != wanted_object_type(trouve):
            logger.info("run.node.incremental_fallback", logical=logical_address, physical=name, reason="object_type_changed")
            effective_mode = RunMode.FULL_REFRESH

    # The INSERT names each column of the target, and the SQL must give the
    # same columns. read_incremental_columns() compares the two lists.
    incremental_columns = None
    if effective_mode == RunMode.INCREMENTAL:
        assert isinstance(trouve, Table)
        try:
            incremental_columns = read_incremental_columns(
                trouve, adapter, addresses.physical, addresses.target
            )
        except ClairError as column_error:
            logger.warning("run.node.failure", logical=logical_address, physical=name, error=str(column_error))
            return _NodeOutcome(
                RunFailure(addresses=addresses, error=str(column_error)),
                downstream_ok=False,
            )
        if not incremental_columns.change.is_empty:
            added_names = [column.name for column in incremental_columns.change.added_columns]
            logger.info("run.node.schema_change", logical=logical_address, physical=name, added_columns=added_names)

    logger.info("run.node.start", logical=logical_address, physical=name, effective_mode=effective_mode.value)
    plan = trouve.build_sql(
        effective_mode,
        run_id,
        target_address=addresses.target,
        target_column_names=incremental_columns.target_column_names if incremental_columns else None,
    )

    # An incremental run changes data that already exists, so the staging
    # table needs that data first. A zero-copy clone gives it in constant time.
    # Each new column goes onto the clone, thus a test that fails leaves the
    # physical table as it was.
    prefix_statements: list[str] = []
    if staging_address is not None and effective_mode == RunMode.INCREMENTAL:
        assert isinstance(trouve, Table)
        prefix_statements.append(
            build_clone_statement(addresses.physical, staging_address, table_options=trouve)
        )
    if incremental_columns is not None:
        prefix_statements.extend(incremental_columns.alter_statements)
    # A build_sql() that gives no statement makes no table, thus no key.
    cluster_by_statements: list[str] = []
    if plan.statements:
        cluster_by_statements = build_cluster_by_statements(
            addresses.target,
            table_options_of(trouve),
            incremental=effective_mode == RunMode.INCREMENTAL,
        )
    # An incremental table keeps its comments, and the two reads above hold
    # them. Clair writes only a comment that differs from the docs.
    current_comments = None
    if incremental_columns is not None and current_object is not None:
        current_comments = WarehouseComments.of(current_object, incremental_columns.target_columns)
    # A staged run writes the comments after the promotion, in _promote_or_keep.
    after_build_statements = cluster_by_statements
    if plan.statements and staging_address is None:
        after_build_statements = [
            *cluster_by_statements,
            *build_comment_statements(addresses.physical, trouve, current_comments),
        ]
    plan = plan.model_copy(
        update={"statements": [*prefix_statements, *plan.statements, *after_build_statements]}
    )

    if not plan.statements:
        return _NodeOutcome(None, downstream_ok=True)

    start = time.monotonic()
    statements: list[Statement] = []
    failed_at = None

    for statement_index, sql in enumerate(plan.statements):
        if is_cancelled():
            statements.append(
                Statement(sql=sql, status=StatementStatus.FAILURE, error=RUN_CANCELLED_ERROR)
            )
        else:
            statements.append(adapter.execute(sql))
        if not statements[-1].success:
            failed_at = statement_index
            break

    # The last statement that writes rows gives the count. The ALTER of the
    # clustering key and the comments come after it.
    last_write_index = len(plan.statements) - len(after_build_statements) - 1
    row_count = statements[min(len(statements) - 1, last_write_index)].row_count

    # The result holds each statement of the plan, thus the reader sees the
    # complete plan and the point where clair stopped.
    statements.extend(Statement(sql=sql) for sql in plan.statements[len(statements):])

    # A failed cleanup leaves an object that the run made for itself, and the
    # data of the Trouve stays correct. Thus it does not fail the node.
    for sql in plan.cleanup_statements:
        cleanup_statement = adapter.execute(sql)
        statements.append(cleanup_statement)
        if not cleanup_statement.success:
            logger.warning("run.node.cleanup_failure", logical=logical_address, physical=name, error=cleanup_statement.error)

    duration = time.monotonic() - start

    # A staged run tests the staging object, then promotes it or keeps it.
    staging_error = ""
    if failed_at is None and staging_address is not None:
        assert after_node_success is not None
        # A cancel can arrive while the tests run, thus clair asks again after
        # them, just before the promotion.
        tests_passed = not is_cancelled() and after_node_success(name, str(staging_address), adapter)
        if is_cancelled():
            staging_error = _cancelled_before_promotion(addresses)
        else:
            promote_statements, staging_error = _promote_or_keep(
                trouve, adapter, addresses, tests_passed, current_object, current_comments
            )
            statements.extend(promote_statements)

    query_ids = [statement.query_id for statement in statements if statement.query_id]

    if failed_at is None and not staging_error:
        logger.info("run.node.success", logical=logical_address, physical=name, duration_seconds=round(duration, 3), query_ids=query_ids)
        downstream_ok = True
        # Without staging the tests run after clair wrote the physical object.
        if staging_address is None and after_node_success is not None:
            downstream_ok = after_node_success(name, str(addresses.physical), adapter)
        return _NodeOutcome(
            RunSuccess(
                addresses=addresses,
                statements=statements,
                effective_run_mode=effective_mode,
                duration_seconds=duration,
                row_count=row_count,
            ),
            downstream_ok,
        )

    if staging_error:
        error_message = staging_error
    else:
        assert failed_at is not None
        error_message = statements[failed_at].error or "unknown error"
        if staging_address is not None:
            error_message = (
                f"{error_message}. Clair keeps the staging object at "
                f"{staging_address}, if the build made one"
            )
    logger.warning("run.node.failure", logical=logical_address, physical=name, duration_seconds=round(duration, 3), error=error_message, query_ids=query_ids)
    return _NodeOutcome(
        RunFailure(
            addresses=addresses,
            statements=statements,
            effective_run_mode=effective_mode,
            error=error_message,
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
        cancellation: The request to stop the run. After it, clair starts no
            new Trouve and gives a RunCancel for each one, and each Trouve that
            runs stops before its next statement and before its promotion.

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
            after_node_success,
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
