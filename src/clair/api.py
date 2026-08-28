"""The Python API of clair.

Each function here does one complete operation on a project: compile it, run it,
test it, or serve the documentation. One function has the name of the command
that calls it, thus `clair run` calls `clair.run()`. A notebook, a test, or
another program calls the same functions::

    import clair

    summary = clair.run("~/projects/analytics", select=["+mydb.analytics.orders"])
    print(summary.succeeded_count, summary.failed_count)
    print(summary.result("mydb.analytics.orders").sql)

Each function gives a result object with the complete data of the operation. No
function writes to stdout, and no function stops the process. A fault raises a
:class:`~clair.exceptions.ClairError`.
"""

from __future__ import annotations

import os
import shutil
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import structlog
import uuid6

from clair.adapters.base import WarehouseAdapter
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.artifacts import (
    CleanOutput,
    find_artifact_runs,
    parse_before_spec,
    select_runs_to_remove,
)
from clair.core.compiler import CompileOutput, write_compile_output
from clair.core.dag import ClairDag, build_dag
from clair.core.discovery import (
    ARTIFACTS_DIR_NAME,
    discover_project,
    find_routing_collisions,
    recompile_for_selection,
)
from clair.core.runner import RunResult, RunSummary, run_project
from clair.core.selector import expand_selectors
from clair.core.test_runner import TestResult, TestSummary, run_tests
from clair.core.validation import ValidationReport, validate_project
from clair.environments.environments import Environment, load_environment
from clair.environments.project_routing import (
    describe_unnamed_environment,
    load_project_routing,
)
from clair.environments.routing import RoutingEntry
from clair.exceptions import EnvironmentsFileNotFoundError
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import TrouveAbc, TrouveType
from clair.web_ui.catalog import build_catalog
from clair.web_ui.server import serve

logger = structlog.get_logger()

Selectors = Sequence[str] | None


def _resolve_routing(project_root: Path, env_name: str) -> RoutingEntry | None:
    """Give the routing entry of *env_name*, and warn about an absent entry.

    A routing file that does not name the active environment is almost always a
    typo. Passthrough routing then writes to the production names, so clair
    tells the user before any SQL runs.
    """
    project_routing = load_project_routing(project_root, env_name)
    warning = describe_unnamed_environment(project_routing, env_name)
    if warning:
        logger.warning("routing.unnamed_environment", env=env_name, detail=warning)
    return project_routing.entry


def _warn_about_routing_collisions(trouves: list[TrouveAbc], env_name: str) -> None:
    """Show each routing collision, before the SQL starts."""
    for physical_address, logical_sources in find_routing_collisions(trouves):
        logger.warning(
            "routing.collision",
            env=env_name,
            physical=physical_address,
            logical_sources=logical_sources,
            detail=(
                "Two or more Trouves write to this one address. Give one Trouve a "
                "different name, change the routing entry, or use select to remove "
                "one Trouve from this run."
            ),
        )


def _select_nodes(
    dag: ClairDag,
    select: Selectors,
    exclude: Selectors,
    keep_sources: bool = False,
) -> list[str]:
    """Apply the selectors and give the physical addresses, in execution order."""
    expanded = expand_selectors(dag, select)
    if keep_sources:
        selected = list(expanded)
    else:
        selected = [n for n in expanded if dag.get_trouve(n).type != TrouveType.SOURCE]
    if exclude:
        excluded = set(expand_selectors(dag, exclude))
        selected = [n for n in selected if n not in excluded]
    return selected


def compile(
    project_dir: str | Path = ".",
    *,
    select: Selectors = None,
    exclude: Selectors = None,
    env: str | None = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    use_staging: bool = True,
) -> CompileOutput:
    """Compile a project and write the SQL to the artifacts directory.

    This function needs no warehouse connection. It gives you the statements of
    each Trouve, the physical address, and the staging address.

    Args:
        project_dir: The root directory of the project.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: The environment name from ~/.clair/environments.yml. None takes
            CLAIR_ENV, then "dev". Clair compiles without an environment if that
            file does not exist.
        run_mode: The run mode for the new SQL statements.
        use_staging: If True, the plan shows the staged path.

    Returns:
        A CompileOutput with one CompiledNodeInfo for each Trouve.

    Raises:
        ClairError: If discovery, routing, or compilation fails.
    """
    project_root = Path(project_dir).expanduser().resolve()
    run_id = uuid6.uuid7().hex

    environment: Environment | None = None
    env_name = env or "dev"
    try:
        env_name, environment = load_environment(env)
    except EnvironmentsFileNotFoundError:
        logger.warning(
            "compile.no_environments_file",
            detail="Clair compiles without an environment. Run `clair init` to make environments.yml.",
        )

    routing = _resolve_routing(project_root, env_name)
    discovered = discover_project(
        project_root, routing=routing, environment=environment, run_mode=run_mode
    )
    _warn_about_routing_collisions(discovered, env_name)
    dag = build_dag(discovered)

    selected = _select_nodes(dag, select, exclude)
    recompile_for_selection(discovered, set(selected))

    source_count = sum(1 for n in dag.nodes if dag.get_trouve(n).type == TrouveType.SOURCE)
    logger.info(
        "compile.start",
        run_id=run_id,
        project=str(project_root),
        trouves=len(dag.nodes) - source_count,
        sources=source_count,
        run_mode=run_mode.value,
    )

    output = write_compile_output(
        dag,
        selected,
        project_root,
        on_node_compiled=lambda node_info: logger.info(
            "compile.node",
            trouve=node_info.name,
            dependencies=node_info.dependencies,
            artifact_file=str(node_info.artifact_path),
        ),
        run_mode=run_mode,
        run_id=run_id,
        use_staging=use_staging,
    )
    output = output.model_copy(update={"env_name": env_name})
    logger.info("compile.complete", run_id=run_id, artifacts_dir=str(output.artifacts_dir))
    return output


def run(
    project_dir: str | Path = ".",
    *,
    select: Selectors = None,
    exclude: Selectors = None,
    env: str | None = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    test: bool = True,
    sample: bool = False,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> RunSummary:
    """Run the Trouves of a project on the warehouse, and test them.

    Clair writes each Trouve to a run-scoped staging address, runs the data
    quality tests there, and promotes the object after the tests pass. The tests
    give that guarantee, so ``test=False`` also stops the staging step: clair
    then writes to each physical address directly.

    Args:
        project_dir: The root directory of the project.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: The environment name from ~/.clair/environments.yml.
        run_mode: full_refresh writes each table again. incremental writes only
            the new data.
        test: If True, clair runs the data quality tests of a Trouve after it
            builds that Trouve.
        sample: If True, clair runs the tests on a sample of each Trouve.
        threads: The number of Trouves that run at one time. None takes the
            thread count of the environment.
        adapter: A connected warehouse adapter. The default makes a
            SnowflakeAdapter, connects it, and closes it at the end. A parallel
            run opens one more connection for each other thread.

    Returns:
        A RunSummary with one RunResult for each Trouve. Each result holds the
        statements, the addresses, the effective run mode, the query IDs, the
        clock time, the row count, and the test results.

    Raises:
        ClairError: If discovery, routing, or the connection fails. A Trouve
            that fails gives a RunResult with the FAILURE status, and raises no
            error, because the other branches of the DAG continue.
    """
    project_root = Path(project_dir).expanduser().resolve()
    run_id = uuid6.uuid7().hex
    use_staging = test
    if not test:
        logger.warning(
            "run.staging_disabled",
            reason="test=False skips the tests that decide the promotion, so clair writes to each physical address directly",
        )

    env_name, environment = load_environment(env)
    # The caller replaces the thread count of the environment.
    thread_count = threads if threads is not None else environment.threads
    profile_defaults = {"warehouse": environment.warehouse, "role": environment.role}
    routing = _resolve_routing(project_root, env_name)
    discovered = discover_project(
        project_root,
        profile_defaults,
        routing=routing,
        environment=environment,
        run_mode=run_mode,
    )
    _warn_about_routing_collisions(discovered, env_name)
    dag = build_dag(discovered)

    selected = _select_nodes(dag, select, exclude)
    if not selected:
        logger.info("run.no_trouves_selected")
        return RunSummary(
            results=[],
            env_name=env_name,
            run_id=run_id,
            project_root=project_root,
            run_mode=run_mode,
        )

    recompile_for_selection(discovered, set(selected))
    write_compile_output(
        dag, selected, project_root, run_mode=run_mode, run_id=run_id, use_staging=use_staging
    )

    # An empty account_locator leaves each query URL empty.
    if not environment.account_locator:
        logger.warning(
            "run.no_account_locator", env=env_name, detail="Clair cannot show the query URLs."
        )

    warehouse_adapter = adapter
    owns_adapter = warehouse_adapter is None
    if warehouse_adapter is None:
        warehouse_adapter = SnowflakeAdapter()
        warehouse_adapter.connect(environment.to_connection_dict())

    test_results_of_node: dict[str, list[TestResult]] = defaultdict(list)

    def on_node_success(
        node_name: str, query_address: str, node_adapter: WarehouseAdapter
    ) -> bool:
        # node_adapter is the connection of the thread that ran the node. A
        # parallel run must not send these test queries on a different
        # connection, because that connection holds a different context.
        node_test_results = run_tests(
            dag,
            [node_name],
            node_adapter,
            use_sample=sample,
            query_addresses={node_name: query_address},
        )
        # list.extend is atomic, thus the threads need no lock here. Each thread
        # writes to the entry of its own node.
        test_results_of_node[node_name].extend(node_test_results)
        return all(test_result.passed for test_result in node_test_results)

    # A connection that no Trouve uses gives nothing. A connection costs one
    # login, not credits: Snowflake bills the warehouse per second while it
    # runs, and an idle session starts no warehouse.
    thread_count = min(thread_count, len(selected))

    try:
        logger.info(
            "run.start",
            run_id=run_id,
            env=env_name,
            project=str(project_root),
            trouves=len(selected),
            run_mode=run_mode.value,
            use_staging=use_staging,
            threads=thread_count,
        )
        results: list[RunResult] = []
        for result in run_project(
            dag,
            selected,
            warehouse_adapter,
            run_mode=run_mode,
            run_id=run_id,
            after_node_success=on_node_success if test else None,
            use_staging=use_staging,
            threads=thread_count,
        ):
            results.append(
                result.model_copy(
                    update={"test_results": test_results_of_node[result.physical_address]}
                )
            )
    finally:
        if owns_adapter:
            warehouse_adapter.close()

    summary = RunSummary(
        results=results,
        env_name=env_name,
        run_id=run_id,
        project_root=project_root,
        run_mode=run_mode,
    )
    logger.info(
        "run.complete",
        run_id=run_id,
        succeeded=summary.succeeded_count,
        failed=summary.failed_count,
        skipped=summary.skipped_count,
    )
    return summary


def test(
    project_dir: str | Path = ".",
    *,
    select: Selectors = None,
    exclude: Selectors = None,
    env: str | None = None,
    sample: bool = False,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> TestSummary:
    """Run the data quality tests of a project on the warehouse.

    Args:
        project_dir: The root directory of the project.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: The environment name from ~/.clair/environments.yml.
        sample: If True, clair runs the tests on a sample of each Trouve, and
            runs no row count test.
        threads: The number of Trouves that clair tests at one time. None takes
            the thread count of the environment.
        adapter: A connected warehouse adapter. The default makes a
            SnowflakeAdapter, connects it, and closes it at the end. A parallel
            test run opens one more connection for each other thread.

    Returns:
        A TestSummary with one TestResult for each test.

    Raises:
        ClairError: If discovery, routing, or the connection fails.
    """
    project_root = Path(project_dir).expanduser().resolve()

    env_name, environment = load_environment(env)
    # The caller replaces the thread count of the environment.
    thread_count = threads if threads is not None else environment.threads
    profile_defaults = {"warehouse": environment.warehouse, "role": environment.role}
    routing = _resolve_routing(project_root, env_name)
    discovered = discover_project(
        project_root, profile_defaults, routing=routing, environment=environment
    )
    dag = build_dag(discovered)

    # Keep each SOURCE, thus a selector can match a SOURCE. run_tests skips it.
    selected = _select_nodes(dag, select, exclude, keep_sources=True)
    if not selected:
        logger.info("test.no_trouves_selected")
        return TestSummary(results=[])

    warehouse_adapter = adapter
    owns_adapter = warehouse_adapter is None
    if warehouse_adapter is None:
        warehouse_adapter = SnowflakeAdapter()
        warehouse_adapter.connect(environment.to_connection_dict())

    # A connection that no Trouve uses gives nothing, and it costs one login.
    thread_count = min(thread_count, len(selected))

    try:
        logger.info(
            "test.start",
            env=env_name,
            project=str(project_root),
            trouves=len(selected),
            threads=thread_count,
        )
        summary = TestSummary(
            results=run_tests(
                dag, selected, warehouse_adapter, use_sample=sample, threads=thread_count
            )
        )
    finally:
        if owns_adapter:
            warehouse_adapter.close()

    if not summary.results:
        logger.info("test.no_tests_found")
        return summary

    logger.info(
        "test.complete",
        passed=summary.passed_count,
        failed=summary.failed_count,
        errors=summary.error_count,
    )
    return summary


def validate(
    project_dir: str | Path = ".",
    *,
    env: str | None = None,
) -> ValidationReport:
    """Apply the project routing entries to each Trouve. This needs no connection.

    The report tells you the physical address problems, the collisions, and each
    address that an author wrote as text. A caller reads the lists, and it
    parses no text::

        report = clair.validate("~/projects/analytics")
        for collision in report.collisions:
            print(collision.physical_address, collision.logical_addresses)

    Args:
        project_dir: The root directory of the project.
        env: The environment name that selects the routing entry. None takes
            CLAIR_ENV, then "dev".

    Returns:
        A ValidationReport. Read ``is_valid`` for the result of the command.

    Raises:
        ClairError: If clair cannot read the project or the routing file.
    """
    project_root = Path(project_dir).expanduser().resolve()
    env_name = env or os.environ.get("CLAIR_ENV") or "dev"
    return validate_project(project_root, env_name)


def clean(
    project_dir: str | Path = ".",
    *,
    before: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> CleanOutput:
    """Remove the compiled artifacts of the old runs. This needs no connection.

    Args:
        project_dir: The root directory of the project.
        before: Remove each run before this time: 'today', 'yesterday',
            'last_week', a duration such as '7d', or an ISO date. None removes
            each run.
        dry_run: If True, clair names the runs and it removes nothing.
        now: The current time, for the *before* value. None takes the clock.

    Returns:
        A CleanOutput that names each run that clair removed.

    Raises:
        InvalidBeforeSpecError: If clair cannot read *before*.
    """
    project_root = Path(project_dir).expanduser().resolve()
    artifacts_dir = project_root / ARTIFACTS_DIR_NAME

    cutoff: datetime | None = None
    if before is not None:
        cutoff = parse_before_spec(before, now or datetime.now(tz=UTC))

    runs = select_runs_to_remove(find_artifact_runs(artifacts_dir), cutoff)
    if not dry_run:
        for run in runs:
            shutil.rmtree(run.path)

    logger.info(
        "clean.complete",
        artifacts_dir=str(artifacts_dir),
        runs=len(runs),
        dry_run=dry_run,
    )
    return CleanOutput(
        artifacts_dir=artifacts_dir,
        artifacts_dir_exists=artifacts_dir.exists(),
        cutoff=cutoff,
        runs=runs,
        dry_run=dry_run,
    )


def catalog(project_dir: str | Path = ".") -> dict:
    """Make the documentation catalog of a project. This needs no connection.

    Args:
        project_dir: The root directory of the project.

    Returns:
        The catalog dictionary: one entry for each Trouve, and the lineage edges.

    Raises:
        ClairError: If discovery fails.
    """
    project_root = Path(project_dir).expanduser().resolve()
    dag = build_dag(discover_project(project_root))
    return build_catalog(dag, project_root)


def docs(
    project_dir: str | Path = ".",
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None:
    """Start the local documentation server. This function does not give control back.

    Args:
        project_dir: The root directory of the project.
        host: The address of the server.
        port: The port of the server.
        open_browser: If True, clair opens the browser on the server address.

    Raises:
        ClairError: If discovery fails.
        OSError: If the port is in use.
    """
    project_catalog = catalog(project_dir)
    logger.info(
        "docs.start",
        project=str(Path(project_dir).expanduser().resolve()),
        host=host,
        port=port,
    )
    serve(project_catalog, host=host, port=port, open_browser=open_browser)


__all__ = ["catalog", "clean", "compile", "docs", "run", "test", "validate"]
