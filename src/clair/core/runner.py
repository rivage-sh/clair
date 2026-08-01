"""The runner. It executes the DAG on Snowflake in topological order."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from enum import StrEnum

import networkx as nx
import pandas as pd
import structlog
from pydantic import BaseModel, model_validator

from clair.adapters.base import WarehouseAdapter
from clair.core.dag import ClairDag, get_executable_nodes
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
        full_name: The full Snowflake object name.
        status: The result of the attempt.
        query_ids: The warehouse query ID of each statement.
        query_urls: The URL of each statement in the Snowflake console.
        error: The error message if the query failed.
        sql: The complete DDL. Clair sets it only for a FAILURE.
        duration_seconds: The clock time of the query.
        skipped_by: The full_name of the upstream Trouve that caused the skip.
            Clair sets it only for a SKIPPED result.
    """

    full_name: str
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
            raise ValueError("SKIPPED results must specify skipped_by")
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
            f"Running {total} Trouve{'s' if total != 1 else ''}...\n"
        )

    @staticmethod
    def render_node(result: RunResult, index: int, total: int) -> str:
        """Make the output lines of one node that completed."""
        lines: list[str] = []

        if result.status == RunStatus.SKIPPED:
            lines.append(f"[{index}/{total}] {result.full_name} ... SKIPPED")
            lines.append(f"      Reason: upstream dependency {result.skipped_by} failed")
        elif result.status == RunStatus.SUCCESS:
            lines.append(
                f"[{index}/{total}] {result.full_name} ... OK ({result.duration_seconds:.1f}s)"
            )
            _append_query_urls(lines, result.query_ids, result.query_urls)
        elif result.status == RunStatus.FAILURE:
            lines.append(
                f"[{index}/{total}] {result.full_name} ... FAILED ({result.duration_seconds:.1f}s)"
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
) -> RunResult:
    """Execute a PandasTrouve. Read the inputs, transform them, write the output.

    Returns a RunResult with the SUCCESS status or the FAILURE status.
    """
    start = time.monotonic()

    # 1. Read each input DataFrame. Clair keeps the order of trouve.inputs.
    input_dataframes: list[pd.DataFrame] = []
    for parameter_name, upstream in zip(trouve.parameter_names(), trouve.upstream_trouves()):
        try:
            input_dataframes.append(adapter.fetch_dataframe(upstream.full_name))
        except Exception as fetch_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
            duration = time.monotonic() - start
            return RunResult(
                full_name=trouve.full_name,
                status=RunStatus.FAILURE,
                error=f"Failed to fetch '{parameter_name}' ({upstream.full_name}): {fetch_error}",
                duration_seconds=duration,
            )

    # 2. Call the transform function. Clair binds each input by position.
    try:
        result_dataframe = trouve.transform(*input_dataframes)
    except Exception as transform_error:  # noqa: BLE001 — the user transform code is unknown
        duration = time.monotonic() - start
        return RunResult(
            full_name=trouve.full_name,
            status=RunStatus.FAILURE,
            error=f"Transform function failed: {transform_error}",
            duration_seconds=duration,
        )

    # 3. Make sure that the result is a DataFrame.
    if not isinstance(result_dataframe, pd.DataFrame):
        duration = time.monotonic() - start
        return RunResult(
            full_name=trouve.full_name,
            status=RunStatus.FAILURE,
            error=(
                f"Transform function must return a pandas DataFrame, "
                f"got {type(result_dataframe).__name__}"
            ),
            duration_seconds=duration,
        )

    # 4. Write the result to Snowflake.
    full_name = trouve.full_name
    name_parts = full_name.split(".")
    if len(name_parts) != 3:
        duration = time.monotonic() - start
        return RunResult(
            full_name=full_name,
            status=RunStatus.FAILURE,
            error=f"Cannot parse full_name '{full_name}' into database.schema.table",
            duration_seconds=duration,
        )

    database_name, schema_name, table_name = name_parts

    try:
        query_result = adapter.write_dataframe(
            dataframe=result_dataframe,
            full_name=full_name,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )
    except Exception as write_error:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
        duration = time.monotonic() - start
        return RunResult(
            full_name=full_name,
            status=RunStatus.FAILURE,
            error=f"Failed to write DataFrame to {full_name}: {write_error}",
            duration_seconds=duration,
        )

    duration = time.monotonic() - start

    if not query_result.success:
        return RunResult(
            full_name=full_name,
            status=RunStatus.FAILURE,
            error=query_result.error or "write_dataframe returned success=False",
            duration_seconds=duration,
        )

    return RunResult(
        full_name=full_name,
        status=RunStatus.SUCCESS,
        query_ids=[query_result.query_id] if query_result.query_id else [],
        query_urls=[query_result.query_url] if query_result.query_url else [],
        duration_seconds=duration,
    )


def run_project(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    after_node_success: Callable[[str], bool] | None = None,
) -> Iterator[RunResult]:
    """Execute the selected Trouves in topological order. Give each result immediately.

    If a node fails, clair marks each node downstream of it as skipped. Then
    clair continues with the other branches.

    after_node_success: an optional callback. Clair calls it after each node
        that succeeds, before the next node starts. Give False to make clair
        treat the node as a failure and skip each node downstream of it. This
        stops the run early when a test fails.
    """
    all_executable = get_executable_nodes(dag)
    to_run = [name for name in all_executable if name in selected]

    skip_reasons: dict[str, str] = {}

    for name in to_run:
        if name in skip_reasons:
            logger.info("run.node.skipped", trouve=name, skipped_by=skip_reasons[name])
            yield RunResult(
                full_name=name,
                status=RunStatus.SKIPPED,
                skipped_by=skip_reasons[name],
            )
            continue

        trouve = dag.get_trouve(name)
        assert trouve.compiled is not None

        context_warehouse = trouve.compiled.config.warehouse if trouve.compiled.config.warehouse and trouve.compiled.config.warehouse.strip() else None
        context_role = trouve.compiled.config.role if trouve.compiled.config.role and trouve.compiled.config.role.strip() else None
        if context_warehouse or context_role:
            try:
                adapter.set_context(warehouse=context_warehouse, role=context_role)
            except Exception as e:  # noqa: BLE001 — each adapter fault becomes a RunResult with the FAILURE status
                logger.warning("run.node.context_error", trouve=name, warehouse=context_warehouse, role=context_role, error=str(e))
                yield RunResult(
                    full_name=name,
                    status=RunStatus.FAILURE,
                    error=f"Failed to set session context: {e}",
                )
                for desc in nx.descendants(dag, name):
                    skip_reasons.setdefault(desc, name)
                continue

        if trouve.type != TrouveType.SOURCE:
            assert trouve.compiled is not None
            routed_parts = trouve.compiled.full_name.split(".")
            if len(routed_parts) >= 2:
                adapter.execute(f"CREATE DATABASE IF NOT EXISTS {routed_parts[0]}")
                adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {routed_parts[0]}.{routed_parts[1]}")

        # A PandasTrouve is different. Clair reads the data, transforms it and
        # writes it. Clair does not execute SQL.
        if trouve.execution_type == ExecutionType.PANDAS:
            assert isinstance(trouve, PandasTrouve)
            logger.info("run.node.start", trouve=name, effective_mode="full_refresh")
            result = _run_pandas_trouve(trouve, adapter)
            yield result

            if result.status == RunStatus.SUCCESS:
                logger.info("run.node.success", trouve=name, duration_seconds=round(result.duration_seconds, 3))
                if after_node_success is not None and not after_node_success(name):
                    for desc in nx.descendants(dag, name):
                        skip_reasons.setdefault(desc, name)
            else:
                logger.warning("run.node.failure", trouve=name, duration_seconds=round(result.duration_seconds, 3), error=result.error)
                for desc in nx.descendants(dag, name):
                    skip_reasons.setdefault(desc, name)
            continue

        assert isinstance(trouve, Trouve)
        effective_mode = resolve_effective_mode(trouve, run_mode)
        # If the target table does not exist yet, change to the full refresh mode.
        if effective_mode == RunMode.INCREMENTAL:
            assert trouve.compiled is not None
            routed_parts = trouve.compiled.full_name.split(".")
            if len(routed_parts) == 3 and not adapter.table_exists(routed_parts[0], routed_parts[1], routed_parts[2]):
                logger.info("run.node.incremental_fallback", trouve=name, reason="table_not_found")
                effective_mode = RunMode.FULL_REFRESH

        logger.info("run.node.start", trouve=name, effective_mode=effective_mode.value)
        statements = trouve.build_sql(effective_mode, run_id)

        if not statements:
            continue

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

        # UPSERT cleanup. If the MERGE at index 1 failed, drop the staging table
        # at index 2 anyway.
        if not all_succeeded and len(statements) == 3 and failed_at == 1:
            adapter.execute(statements[2])

        if all_succeeded:
            logger.info("run.node.success", trouve=name, duration_seconds=round(duration, 3), query_ids=query_ids)
            yield RunResult(
                full_name=name,
                status=RunStatus.SUCCESS,
                query_ids=query_ids,
                query_urls=query_urls,
                duration_seconds=duration,
            )
            if after_node_success is not None and not after_node_success(name):
                for desc in nx.descendants(dag, name):
                    skip_reasons.setdefault(desc, name)
        else:
            assert last_result is not None
            logger.warning("run.node.failure", trouve=name, duration_seconds=round(duration, 3), error=last_result.error, query_ids=query_ids)
            yield RunResult(
                full_name=name,
                status=RunStatus.FAILURE,
                query_ids=query_ids,
                query_urls=query_urls,
                error=last_result.error or "",
                sql=statements,
                duration_seconds=duration,
            )
            for desc in nx.descendants(dag, name):
                skip_reasons.setdefault(desc, name)


def format_run_output(results: list[RunResult], env_name: str) -> RunSummary:
    """Make a RunSummary from the run results.

    Args:
        results: A list of RunResult objects.
        env_name: The name of the active environment.

    Returns:
        A RunSummary. It holds the data and supplies a .render() method.
    """
    return RunSummary(results=results, env_name=env_name)
