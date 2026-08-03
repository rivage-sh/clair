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
from clair.core.strict import (
    build_clone_statement,
    build_drop_staging_statement,
    build_promote_statement,
    strict_staging_name,
)
from clair.exceptions import ClairError, RunError
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
    target_full_name: str | None = None,
) -> RunResult:
    """Execute a df_fn Trouve: fetch inputs, transform, write output.

    Args:
        trouve: The compiled df_fn Trouve to run.
        adapter: Connected warehouse adapter.
        target_full_name: Object to write into, overriding the Trouve's routed
            name. Strict mode uses this to write into a staging table. The
            returned RunResult always reports the routed name.

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
    write_target = target_full_name or full_name
    name_parts = write_target.split(".")
    if len(name_parts) != 3:
        duration = time.monotonic() - start
        return RunResult(
            full_name=full_name,
            status=RunStatus.FAILURE,
            error=f"Cannot parse full_name '{write_target}' into database.schema.table",
            duration_seconds=duration,
        )

    database_name, schema_name, table_name = name_parts

    try:
        query_result = adapter.write_dataframe(
            dataframe=result_dataframe,
            full_name=write_target,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )
    except Exception as write_error:
        duration = time.monotonic() - start
        return RunResult(
            full_name=full_name,
            status=RunStatus.FAILURE,
            error=f"Failed to write DataFrame to {write_target}: {write_error}",
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


def _promote_or_retain(
    trouve: Trouve,
    adapter: WarehouseAdapter,
    staging_name: str,
    target_name: str,
    tests_passed: bool,
) -> tuple[list[str], list[str], str]:
    """Finish a strict-mode node: promote the staging object, or leave it for inspection.

    A rejected candidate is never dropped. It is the only copy of what the run
    produced, and rebuilding it means re-running every upstream Trouve -- so it is
    left in place and its name reported, ready to be queried directly.

    Returns:
        (query_ids, query_urls, error). The error is empty when the staging object
        was promoted successfully.
    """
    query_ids: list[str] = []
    query_urls: list[str] = []

    def _execute(statement: str) -> str:
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
            f"strict mode: tests failed, {target_name} left unchanged "
            f"(rejected candidate retained at {staging_name})",
        )

    assert trouve.compiled is not None
    promote_error = _execute(
        build_promote_statement(
            trouve.type,
            staging_name=staging_name,
            target_name=target_name,
            resolved_sql=trouve.compiled.resolved_sql,
        )
    )
    if promote_error:
        return (
            query_ids,
            query_urls,
            f"strict mode: tests passed but promotion failed: {promote_error} "
            f"(candidate retained at {staging_name})",
        )

    # The target now holds the tested data, so the staging copy is redundant.
    # A failure here is untidy, not incorrect -- the node still succeeded.
    drop_result = adapter.execute(build_drop_staging_statement(trouve.type, staging_name))
    if not drop_result.success:
        logger.warning(
            "run.node.staging_drop_failed",
            trouve=target_name,
            staging=staging_name,
            error=drop_result.error,
        )

    return query_ids, query_urls, ""


def run_project(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    run_id: str = "",
    after_node_success: Callable[[str, str], bool] | None = None,
    strict: bool = False,
) -> Iterator[RunResult]:
    """Execute selected Trouves in topological order, yielding each result as it completes.

    On failure: marks the failed node and all downstream dependents as skipped,
    then continues with unrelated branches.

    after_node_success: optional callback invoked after each successful node, before
        the next node runs. Called with (node_name, physical_name) where
        physical_name is the object that was actually written -- the staging object
        under strict mode, the routed name otherwise. Return False to treat the node
        as failed for downstream dependency purposes (circuit breaker for eager
        testing).
    strict: when True, materialize each node into a run-scoped staging object, let
        after_node_success test it, and only then promote it into its real name. A
        node whose tests fail leaves its target untouched and is reported as a
        FAILURE. Requires after_node_success.
    """
    if strict and after_node_success is None:
        raise RunError("strict mode requires tests; after_node_success must be provided")

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

        routed_name = trouve.compiled.full_name

        if trouve.type != TrouveType.SOURCE:
            routed_parts = routed_name.split(".")
            if len(routed_parts) >= 2:
                adapter.execute(f"CREATE DATABASE IF NOT EXISTS {routed_parts[0]}")
                adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {routed_parts[0]}.{routed_parts[1]}")

        # Strict mode materializes into a run-scoped sibling object; the real name
        # is only written once the tests against that object have passed.
        staging_name: str | None = None
        if strict:
            try:
                staging_name = strict_staging_name(routed_name, run_id)
            except ClairError as naming_error:
                logger.warning("run.node.failure", trouve=name, error=str(naming_error))
                yield RunResult(
                    full_name=name,
                    status=RunStatus.FAILURE,
                    error=str(naming_error),
                )
                for desc in nx.descendants(dag, name):
                    skip_reasons.setdefault(desc, name)
                continue
        write_target = staging_name or routed_name

        query_ids: list[str] = []
        query_urls: list[str] = []
        statements: list[str] | None = None

        # Branch: df_fn Trouves use fetch/transform/write instead of SQL execution
        if trouve.df_fn is not None:
            logger.info("run.node.start", trouve=name, effective_mode="full_refresh", target=write_target)
            df_result = _run_df_fn_trouve(trouve, adapter, target_full_name=write_target)
            report_name = df_result.full_name
            duration = df_result.duration_seconds
            query_ids.extend(df_result.query_ids)
            query_urls.extend(df_result.query_urls)
            materialized = df_result.status == RunStatus.SUCCESS
            error = df_result.error
        else:
            report_name = name

            effective_mode = resolve_effective_mode(trouve, run_mode)
            # Incremental fallback: if target table doesn't exist yet, run full refresh
            if effective_mode == RunMode.INCREMENTAL:
                routed_parts = routed_name.split(".")
                if len(routed_parts) == 3 and not adapter.table_exists(routed_parts[0], routed_parts[1], routed_parts[2]):
                    logger.info("run.node.incremental_fallback", trouve=name, reason="table_not_found")
                    effective_mode = RunMode.FULL_REFRESH

            logger.info("run.node.start", trouve=name, effective_mode=effective_mode.value, target=write_target)
            statements = trouve.build_sql(effective_mode, run_id, target_name=write_target)

            if not statements:
                continue

            # An incremental build needs its prior state; a zero-copy clone puts it
            # in the staging table in constant time.
            if staging_name is not None and effective_mode == RunMode.INCREMENTAL:
                statements = [build_clone_statement(routed_name, staging_name)] + statements

            start = time.monotonic()
            last_result = None
            materialized = True
            failed_at = None

            for stmt_idx, stmt in enumerate(statements):
                query_result = adapter.execute(stmt)
                last_result = query_result
                if query_result.query_id:
                    query_ids.append(query_result.query_id)
                if query_result.query_url:
                    query_urls.append(query_result.query_url)
                if not query_result.success:
                    materialized = False
                    failed_at = stmt_idx
                    break

            duration = time.monotonic() - start

            # UPSERT cleanup: if the MERGE failed, still drop the merge staging table.
            # UPSERT always emits (create staging, merge, drop staging) as the last
            # three statements -- strict mode may prepend a clone before them.
            if not materialized and len(statements) >= 3:
                merge_index = len(statements) - 2
                if failed_at == merge_index:
                    adapter.execute(statements[merge_index + 1])

            error = "" if materialized else ((last_result.error if last_result else "") or "")

        # Strict mode: test the staging object, then promote it or leave it be.
        if staging_name is not None and materialized:
            assert after_node_success is not None
            tests_passed = after_node_success(name, staging_name)
            promote_ids, promote_urls, strict_error = _promote_or_retain(
                trouve, adapter, staging_name, routed_name, tests_passed
            )
            query_ids.extend(promote_ids)
            query_urls.extend(promote_urls)
            if strict_error:
                materialized = False
                error = strict_error
        elif staging_name is not None and not materialized:
            # Whatever the build managed to produce is worth keeping around.
            error = f"{error} (strict staging object left at {staging_name} if it was created)"

        if materialized:
            logger.info("run.node.success", trouve=name, duration_seconds=round(duration, 3), query_ids=query_ids)
            yield RunResult(
                full_name=report_name,
                status=RunStatus.SUCCESS,
                query_ids=query_ids,
                query_urls=query_urls,
                duration_seconds=duration,
            )
            # Non-strict: tests run after the target has already been written.
            if not strict and after_node_success is not None and not after_node_success(name, routed_name):
                for desc in nx.descendants(dag, name):
                    skip_reasons.setdefault(desc, name)
        else:
            logger.warning("run.node.failure", trouve=name, duration_seconds=round(duration, 3), error=error, query_ids=query_ids)
            yield RunResult(
                full_name=report_name,
                status=RunStatus.FAILURE,
                query_ids=query_ids,
                query_urls=query_urls,
                error=error,
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
