"""Runner -- execute the DAG against Snowflake in topological order."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Iterator
from enum import StrEnum
from typing import Any

import pandas as pd
import networkx as nx
import structlog
from pydantic import BaseModel, model_validator

from clair.adapters.base import WarehouseAdapter
from clair.core.dag import ClairDag, get_executable_nodes
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import Trouve, TrouveType


class RunStatus(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"


class RunResult(BaseModel):
    """Result of materializing a single Trouve in the warehouse.

    Attributes:
        full_name: Fully-qualified Snowflake object name.
        status: Outcome of the materialization attempt.
        query_ids: Warehouse query IDs for each executed statement.
        query_urls: URLs to each statement in the Snowflake console.
        error: Error message if the query failed.
        sql: The full DDL executed; only set on FAILURE.
        duration_seconds: Wall-clock time for the query.
        skipped_by: full_name of the upstream that caused the skip; only set on SKIPPED.
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
    def _check_skipped_has_cause(self) -> 'RunResult':
        if self.status == RunStatus.SKIPPED and not self.skipped_by:
            raise ValueError("SKIPPED results must specify skipped_by")
        return self


def _append_query_urls(lines: list[str], query_ids: list[str], query_urls: list[str]) -> None:
    """Append query ID and URL lines, labelled [i/n] when there are multiple statements."""
    n = len(query_ids)
    for i, (qid, url) in enumerate(zip(query_ids, query_urls), 1):
        prefix = f" [{i}/{n}]" if n > 1 else ""
        lines.append(f"      Query ID{prefix}: {qid}")
        lines.append(f"      URL{prefix}: {url}")


class RunSummary(BaseModel):
    """Structured result of a run operation."""

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
        """Render the run header before any nodes execute."""
        return (
            f"=== Clair Run (env: {env_name}) ===\n"
            f"\n"
            f"Running {total} Trouve{'s' if total != 1 else ''}...\n"
        )

    @staticmethod
    def render_node(result: RunResult, index: int, total: int) -> str:
        """Render the output lines for a single completed node."""
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
        """Render the final summary line."""
        return f"=== Done: {succeeded} succeeded, {failed} failed, {skipped} skipped ==="

    def render(self) -> str:
        """Produce the formatted summary string for stdout."""
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
    """Determine the effective run mode for a Trouve without adapter checks.

    Shared by the compiler (no connection) and the runner (which additionally
    checks table existence before committing to incremental).
    """
    if trouve.type == TrouveType.VIEW:
        return RunMode.FULL_REFRESH
    if trouve.run_config.run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    if cli_run_mode != RunMode.INCREMENTAL:
        return RunMode.FULL_REFRESH
    return RunMode.INCREMENTAL


def _run_df_fn_trouve(
    trouve: Trouve,
    adapter: WarehouseAdapter,
) -> RunResult:
    """Execute a df_fn Trouve: fetch inputs, transform, write output.

    Returns a RunResult with SUCCESS or FAILURE status.
    """
    start = time.monotonic()

    # 1. Fetch all input DataFrames via inspect.signature
    dataframe_kwargs: dict[str, Any] = {}
    for param_name, param in inspect.signature(trouve.df_fn).parameters.items():
        if isinstance(param.default, Trouve):
            try:
                dataframe_kwargs[param_name] = adapter.fetch_dataframe(param.default.full_name)
            except Exception as fetch_error:
                duration = time.monotonic() - start
                return RunResult(
                    full_name=trouve.full_name,
                    status=RunStatus.FAILURE,
                    error=f"Failed to fetch '{param_name}' ({param.default.full_name}): {fetch_error}",
                    duration_seconds=duration,
                )

    # 2. Call the df_fn function
    try:
        result_dataframe = trouve.df_fn(**dataframe_kwargs)
    except Exception as transform_error:
        duration = time.monotonic() - start
        return RunResult(
            full_name=trouve.full_name,
            status=RunStatus.FAILURE,
            error=f"Transform function failed: {transform_error}",
            duration_seconds=duration,
        )

    # 3. Validate the result is a DataFrame
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

    # 4. Write the result to Snowflake
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
    except Exception as write_error:
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
    """Execute selected Trouves in topological order, yielding each result as it completes.

    On failure: marks the failed node and all downstream dependents as skipped,
    then continues with unrelated branches.

    after_node_success: optional callback invoked after each successful node, before
        the next node runs. Return False to treat the node as failed for downstream
        dependency purposes (circuit breaker for eager testing).
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
            except Exception as e:
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

        # Branch: df_fn Trouves use fetch/transform/write instead of SQL execution
        if trouve.df_fn is not None:
            logger.info("run.node.start", trouve=name, effective_mode="full_refresh")
            result = _run_df_fn_trouve(trouve, adapter)
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

        effective_mode = resolve_effective_mode(trouve, run_mode)
        # Incremental fallback: if target table doesn't exist yet, run full refresh
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

        # UPSERT cleanup: if MERGE (stmt index 1) failed, still drop staging (stmt index 2)
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
    """Build a structured RunSummary from run results.

    Args:
        results: List of RunResult objects.
        env_name: Name of the active environment.

    Returns:
        A RunSummary with structured data and a .render() method.
    """
    return RunSummary(results=results, env_name=env_name)
