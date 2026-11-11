"""The Python API of clair.

Each function here does one complete operation on a project: compile it, run it,
test it, or serve the documentation. One function has the name of the command
that calls it, thus `clair run` calls `clair.run()`. A notebook, a test, or
another program calls the same functions::

    import clair
    from clair import Authenticator, Connection, Environment

    environment = Environment(
        name="dev",
        connection=Connection(account="ab12345", user="analyst", warehouse="compute_wh"),
        authentication=Authenticator(authenticator="externalbrowser"),
    )
    summary = clair.run(
        "~/projects/analytics", env=environment, select=["+mydb.analytics.orders"]
    )
    print(summary.succeeded_count, summary.failed_count)
    print(summary.result("mydb.analytics.orders").statements)

`run`, `compile` and `test` accept the parsed Environment. They read no file, and
they need no home directory: the CLI parses ~/.clair/environments.yml, and it
gives the object to these functions.

Each function gives a result object with the complete data of the operation. No
function writes to stdout, no function writes an artifact file, and no function
stops the process. A fault raises a :class:`~clair.exceptions.ClairError`. The
caller decides where the bytes go: the CLI gives each result to a writer of
:mod:`clair.core.artifacts`. ``start_run`` gives a RunHandle, thus the caller
gets each result of a run while the run continues.
"""

from __future__ import annotations

import shutil
from collections import defaultdict
from collections.abc import Generator, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple, Self

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
from clair.core.compiler import CompileOutput, build_compile_output
from clair.core.dag import ClairDag, build_dag
from clair.core.discovery import ARTIFACTS_DIR_NAME, discover_project
from clair.core.references import recompile_for_selection
from clair.core.runner import (
    RunCancel,
    RunCancellation,
    RunResult,
    RunSkip,
    RunSummary,
    run_project,
)
from clair.core.selector import expand_selectors
from clair.core.test_runner import SampleScope, TestResult, TestSummary, run_tests
from clair.core.validation import ValidationReport, validate_project
from clair.environments.environments import Environment
from clair.environments.project_routing import find_project_root, routing_entry_of
from clair.environments.routing import (
    detect_routing_collisions_of_trouves,
)
from clair.exceptions import RunError, SampleTargetsProductionError
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS, TrouveAbc
from clair.trouves.trouve_type import TrouveType
from clair.web_ui.catalog import build_catalog
from clair.web_ui.server import serve

logger = structlog.get_logger()

Selectors = Sequence[str] | None

# The environment of an operation. An Environment holds the connection settings.
# A name selects the routing entry only, thus a function that needs no
# connection accepts it. No operation gives a default: an environment without a
# routing entry writes to the production addresses.
EnvSpec = Environment | str


def _resolve_project_root(project_dir: str | Path | None) -> Path:
    """Give the project root as an absolute path.

    None starts the root search: clair walks up from the working directory to
    the first ``__routing__.py``, in the same way that git finds ``.git``.
    """
    if project_dir is None:
        return find_project_root(Path.cwd())
    return Path(project_dir).expanduser().resolve()


def _env_name_of(env: EnvSpec) -> str:
    """Give the environment name of *env*."""
    if isinstance(env, Environment):
        return env.name
    return env


def _warn_about_routing_collisions(trouves: list[TrouveAbc], env_name: str) -> None:
    """Show each routing collision, before the SQL starts."""
    for physical_address, logical_sources in detect_routing_collisions_of_trouves(
        trouves
    ):
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


class _DiscoveredProject(NamedTuple):
    """The project after discovery: each Trouve, compiled to its logical addresses."""

    project_root: Path
    env_name: str
    trouves: list[TrouveAbc]
    dag: ClairDag


def _discover(
    project_dir: str | Path | None, env: EnvSpec, run_mode: RunMode = RunMode.FULL_REFRESH
) -> _DiscoveredProject:
    """Find the project, apply the routing of *env*, discover each Trouve, and build the DAG.

    An Environment gives its warehouse and its role to each Trouve that names
    none. A name gives the routing entry only.
    """
    project_root = _resolve_project_root(project_dir)
    environment = env if isinstance(env, Environment) else None
    env_name = _env_name_of(env)
    profile_defaults = (
        None
        if environment is None
        else {"warehouse": environment.connection.warehouse, "role": environment.connection.role}
    )
    routing = routing_entry_of(project_root, env_name)
    trouves = discover_project(
        project_root,
        profile_defaults,
        routing=routing,
        environment=environment,
        run_mode=run_mode,
    )
    _warn_about_routing_collisions(trouves, env_name)
    return _DiscoveredProject(project_root, env_name, trouves, build_dag(trouves))


class _Plan(NamedTuple):
    """The SQL of a run: the selected Trouves, with each reference resolved for the selection."""

    project: _DiscoveredProject
    selected: list[str]
    # The row limit of a sampled plan, or None for a plan that reads each row.
    sample_rows: int | None


def _plan(
    project_dir: str | Path | None,
    *,
    env: EnvSpec,
    select: Selectors,
    exclude: Selectors,
    run_mode: RunMode,
    sample: bool,
    sample_rows: int,
) -> _Plan:
    """Give the plan that clair.compile() writes and clair.run() executes.

    The two operations share this function, thus a compile always shows the SQL
    that a run with the same arguments sends.

    Raises:
        SampleTargetsProductionError: If *sample* is True, and a selected
            Trouve writes to its logical address.
        ValueError: If *sample_rows* is less than 1.
    """
    _require_a_positive_sample_rows(sample_rows)
    project = _discover(project_dir, env, run_mode)
    selected = _select_nodes(project.dag, select, exclude)
    plan_sample_rows = sample_rows if sample else None
    if sample:
        _refuse_a_sampled_production_write(project.dag, selected, project.env_name)
    recompile_for_selection(project.trouves, set(selected), sample_rows=plan_sample_rows)
    return _Plan(project, selected, plan_sample_rows)


def compile(
    project_dir: str | Path | None = None,
    *,
    env: EnvSpec,
    select: Selectors = None,
    exclude: Selectors = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    use_staging: bool = True,
    sample: bool = False,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
) -> CompileOutput:
    """Compile a project, and give the SQL that a run sends. This writes no file.

    This function needs no warehouse connection. It gives you the statements of
    each Trouve, the physical address, and the staging address. Give the output
    to :func:`clair.core.artifacts.write_compile_output` to write the files.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: An Environment, or an environment name. This function reads no
            file: the caller parses environments.yml, or it makes the
            Environment itself. A name selects the routing entry, and it leaves
            ``clair.env`` empty in each Trouve module.
        run_mode: The run mode for the new SQL statements.
        use_staging: If True, the plan shows the staged path.
        sample: If True, the plan shows the SQL of ``clair.run(sample=True)``:
            each reference to a Trouve outside the selection reads
            ``Trouve.sample()``. The rule for production is the same as in
            :func:`run`.
        sample_rows: The row limit of a sampled read, for each Trouve without
            a ``sample_sql``.

    Returns:
        A CompileOutput with one CompiledNodeInfo for each Trouve.

    Raises:
        SampleTargetsProductionError: If *sample* is True, and a selected
            Trouve writes to its logical address.
        ValueError: If *sample_rows* is less than 1.
        ClairError: If discovery, routing, or compilation fails.
    """
    plan = _plan(
        project_dir,
        env=env,
        select=select,
        exclude=exclude,
        run_mode=run_mode,
        sample=sample,
        sample_rows=sample_rows,
    )
    project_root, env_name, _, dag = plan.project
    run_id = uuid6.uuid7().hex

    source_count = sum(1 for n in dag.nodes if dag.get_trouve(n).type == TrouveType.SOURCE)
    logger.info(
        "compile.start",
        run_id=run_id,
        project=str(project_root),
        trouves=len(dag.nodes) - source_count,
        sources=source_count,
        run_mode=run_mode.value,
        sample_rows=plan.sample_rows,
    )

    output = build_compile_output(
        dag, plan.selected, run_mode=run_mode, run_id=run_id, use_staging=use_staging
    ).model_copy(
        update={
            "project_root": project_root,
            "env_name": env_name,
            "sample_rows": plan.sample_rows,
        }
    )
    for node_info in output.compiled_nodes:
        logger.info(
            "compile.node",
            trouve=str(node_info.addresses.physical),
            dependencies=node_info.dependencies,
        )
    logger.info("compile.complete", run_id=run_id, trouves=len(output.compiled_nodes))
    return output


class RunHandle:
    """One planned run. Iterate over it to execute the run.

    :func:`start_run` makes the handle. The handle gives each RunResult when its
    Trouve completes, thus the caller decides what to do with each result while
    the run continues::

        with clair.start_run(project_dir, env=environment) as run:
            for result in run:
                print(result.addresses.physical, result.status)
        summary = run.summary

    The handle executes the run one time. It opens the connection at the first
    result, and it closes the connection after the last result, or when the
    ``with`` block ends. Thus a loop that stops early closes each connection.

    Attributes:
        run_id: The UUIDv7 hex string that identifies this run. The staging
            addresses hold it.
    """

    def __init__(
        self,
        plan: _Plan,
        *,
        environment: Environment,
        run_id: str,
        run_mode: RunMode,
        test: bool,
        sample: bool,
        threads: int,
        adapter: WarehouseAdapter | None,
    ) -> None:
        self.run_id = run_id
        self._plan = plan
        self._environment = environment
        self._run_mode = run_mode
        self._test = test
        self._sample = sample
        self._threads = threads
        self._adapter = adapter
        self._owns_adapter = adapter is None
        self._results: list[RunResult] = []
        self._result_generator: Generator[RunResult] | None = None
        self._complete = False
        self._cancellation = RunCancellation()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exception_info: object) -> None:
        self.close()

    def __iter__(self) -> Iterator[RunResult]:
        """Execute the run, and give each result when its Trouve completes.

        Raises:
            RunError: If the caller iterates over the handle a second time.
            ClairError: If the connection fails. A Trouve that fails gives a
                RunFailure, and raises no error.
        """
        if self._result_generator is not None:
            raise RunError("A run handle executes one time. Call clair.start_run() again.")
        self._result_generator = self._execute()
        return self._result_generator

    @property
    def summary(self) -> RunSummary:
        """The RunSummary of the complete run.

        Raises:
            RunError: If the loop did not get the last result. A summary of
                part of a run would look complete, thus clair gives none.
        """
        if not self._complete:
            raise RunError(
                "The run did not complete. Iterate over each result before you read the summary."
            )
        project = self._plan.project
        return RunSummary(
            results=self._results,
            env_name=project.env_name,
            run_id=self.run_id,
            project_root=project.project_root,
            run_mode=self._run_mode,
            sample_rows=self._plan.sample_rows,
        )

    def cancel(self) -> None:
        """Stop the run. Call it from another thread, or from a signal handler.

        clair starts no new Trouve, cancels the statement of each connection,
        and promotes nothing more. Each physical object keeps its data or its
        promoted data. The loop then gives a result for each Trouve, with the
        status CANCELLED for each one that did not start, and it ends. Thus
        ``summary`` stays complete, and a retry can select from it.
        """
        self._cancellation.cancel()

    @property
    def is_cancelled(self) -> bool:
        """True after a call of cancel()."""
        return self._cancellation.is_cancelled

    def close(self) -> None:
        """Stop the run, and close each connection that the handle opened."""
        if self._result_generator is not None:
            # The finally block of run_project closes the thread pool.
            self._result_generator.close()
        self._close_adapter()

    def _close_adapter(self) -> None:
        if self._owns_adapter and self._adapter is not None:
            self._adapter.close()
            self._adapter = None

    def _connect(self) -> WarehouseAdapter:
        """Give the adapter of the caller, or make a SnowflakeAdapter and connect it."""
        if self._adapter is None:
            snowflake_adapter = SnowflakeAdapter()
            snowflake_adapter.connect(self._environment)
            self._adapter = snowflake_adapter
        return self._adapter

    def _execute(self) -> Generator[RunResult]:
        project_root, env_name, _, dag = self._plan.project
        selected = self._plan.selected
        if not selected:
            logger.info("run.no_trouves_selected")
            self._complete = True
            return

        # An empty account_locator leaves each query URL empty.
        if not self._environment.connection.account_locator:
            logger.warning(
                "run.no_account_locator", env=env_name, detail="Clair cannot show the query URLs."
            )

        test_results_of_node: dict[str, list[TestResult]] = defaultdict(list)
        sample_scope = SampleScope.RUN_INPUTS if self._sample else SampleScope.NONE

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
                sample_scope=sample_scope,
                query_addresses={node_name: query_address},
            )
            # list.extend is atomic, thus the threads need no lock here. Each
            # thread writes to the entry of its own node.
            test_results_of_node[node_name].extend(node_test_results)
            return not any(test_result.is_blocking for test_result in node_test_results)

        # A connection that no Trouve uses gives nothing. A connection costs one
        # login, not credits: Snowflake bills the warehouse per second while it
        # runs, and an idle session starts no warehouse.
        thread_count = min(self._threads, len(selected))

        try:
            warehouse_adapter = self._connect()
            logger.info(
                "run.start",
                run_id=self.run_id,
                env=env_name,
                project=str(project_root),
                trouves=len(selected),
                run_mode=self._run_mode.value,
                use_staging=self._test,
                threads=thread_count,
            )
            for result in run_project(
                dag,
                selected,
                warehouse_adapter,
                run_mode=self._run_mode,
                run_id=self.run_id,
                after_node_success=on_node_success if self._test else None,
                use_staging=self._test,
                threads=thread_count,
                environment_name=env_name,
                cancellation=self._cancellation,
            ):
                # A Trouve that did not start ran no test, thus its result has no test results.
                if not isinstance(result, RunSkip | RunCancel):
                    result = result.model_copy(
                        update={"test_results": test_results_of_node[str(result.addresses.physical)]}
                    )
                self._results.append(result)
                yield result
            self._complete = True
        finally:
            # A generator cannot close itself, thus this block closes the
            # adapter only. close() closes the generator.
            self._close_adapter()

        summary = self.summary
        logger.info(
            "run.complete",
            run_id=self.run_id,
            succeeded=summary.succeeded_count,
            failed=summary.failed_count,
            skipped=summary.skipped_count,
        )


def start_run(
    project_dir: str | Path | None = None,
    *,
    env: Environment,
    select: Selectors = None,
    exclude: Selectors = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    test: bool = True,
    sample: bool = False,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> RunHandle:
    """Plan a run, and give the RunHandle that executes it. This opens no connection.

    Iterate over the handle to get each RunResult when its Trouve completes. The
    arguments are the arguments of :func:`run`. Use this function when you do
    something with each result while the run continues: ``clair run`` writes the
    artifact file of each Trouve. Use :func:`run` when the summary is enough.

    This function does each step that can fail before the connection:
    discovery, routing, the selection, and the rule for a sample in
    production. Thus a fault in the project raises here, and not in the loop.

    Raises:
        SampleTargetsProductionError: If *sample* is True, and a selected
            Trouve writes to its logical address.
        ValueError: If *sample_rows* is less than 1.
        ClairError: If discovery or routing fails.
    """
    if not test:
        logger.warning(
            "run.staging_disabled",
            reason="test=False skips the tests that decide the promotion, so clair writes to each physical address directly",
        )
    plan = _plan(
        project_dir,
        env=env,
        select=select,
        exclude=exclude,
        run_mode=run_mode,
        sample=sample,
        sample_rows=sample_rows,
    )
    if sample:
        logger.info("run.sample", sample_rows=sample_rows)
    return RunHandle(
        plan,
        environment=env,
        run_id=uuid6.uuid7().hex,
        run_mode=run_mode,
        test=test,
        sample=sample,
        # The caller replaces the thread count of the environment.
        threads=threads if threads is not None else env.threads,
        adapter=adapter,
    )


def run(
    project_dir: str | Path | None = None,
    *,
    env: Environment,
    select: Selectors = None,
    exclude: Selectors = None,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    test: bool = True,
    sample: bool = False,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> RunSummary:
    """Run the Trouves of a project on the warehouse, and test them.

    Clair writes each Trouve to a run-scoped staging address, runs the data
    quality tests there, and promotes the object after the tests pass. The tests
    give that guarantee, so ``test=False`` also stops the staging step: clair
    then writes to each physical address directly.

    This function writes no file. To get each result while the run continues,
    use :func:`start_run`.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: The Environment that holds the connection settings, and that names
            the routing entry. This function reads no file: the caller parses
            environments.yml, or it makes the Environment itself.
        run_mode: full_refresh writes each table again. incremental writes only
            the new data.
        test: If True, clair runs the data quality tests of a Trouve after it
            builds that Trouve.
        sample: If True, each reference to a Trouve that the run does not
            build reads ``Trouve.sample()``: its ``sample_sql``, or
            *sample_rows* rows. Clair skips each test that needs the complete
            data. Each selected Trouve must write to an address that routing
            changed, because a sample must never replace a production object.
        sample_rows: The row limit of a sampled read, for each Trouve without
            a ``sample_sql``.
        threads: The number of Trouves that run at one time. None takes the
            thread count of the environment.
        adapter: A connected warehouse adapter. The default makes a
            SnowflakeAdapter, connects it, and closes it at the end. A parallel
            run opens one more connection for each other thread.

    Returns:
        A RunSummary with one RunResult for each Trouve. Each result holds the
        addresses, the statements, the effective run mode, the clock time, the
        row count, and the test results.

    Raises:
        SampleTargetsProductionError: If *sample* is True, and a selected
            Trouve writes to its logical address. Clair raises it before it
            connects.
        ValueError: If *sample_rows* is less than 1.
        ClairError: If discovery, routing, or the connection fails. A Trouve
            that fails gives a RunResult with the FAILURE status, and raises no
            error, because the other branches of the DAG continue.
    """
    with start_run(
        project_dir,
        env=env,
        select=select,
        exclude=exclude,
        run_mode=run_mode,
        test=test,
        sample=sample,
        sample_rows=sample_rows,
        threads=threads,
        adapter=adapter,
    ) as run_handle:
        for _ in run_handle:
            pass
    return run_handle.summary


def _require_a_positive_sample_rows(sample_rows: int) -> None:
    if sample_rows < 1:
        raise ValueError(f"sample_rows must be 1 or more, but it is {sample_rows}")


def _refuse_a_sampled_production_write(
    dag: ClairDag, selected: Sequence[str], env_name: str
) -> None:
    """Stop a sampled run that would write to a production address.

    A Trouve writes to production when routing gives it its logical address.
    A SOURCE writes nothing, thus it is never a fault.

    Raises:
        SampleTargetsProductionError: If a selected Trouve writes to its
            logical address.
    """
    production_addresses: list[str] = []
    for node_name in selected:
        trouve = dag.get_trouve(node_name)
        if trouve.type == TrouveType.SOURCE or trouve.compiled is None:
            continue
        if trouve.compiled.physical_address == trouve.logical_address:
            production_addresses.append(str(trouve.logical_address))
    if production_addresses:
        raise SampleTargetsProductionError(env_name, sorted(production_addresses))


def test(
    project_dir: str | Path | None = None,
    *,
    env: Environment,
    select: Selectors = None,
    exclude: Selectors = None,
    sample: bool = False,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    threads: int | None = None,
    adapter: WarehouseAdapter | None = None,
) -> TestSummary:
    """Run the data quality tests of a project on the warehouse.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
        select: The patterns that select Trouves. None selects each Trouve.
        exclude: The patterns that remove Trouves, after the selection.
        env: The Environment that holds the connection settings, and that names
            the routing entry. This function reads no file: the caller parses
            environments.yml, or it makes the Environment itself.
        sample: If True, each test reads ``Trouve.sample()`` of its Trouve:
            the ``sample_sql``, or *sample_rows* rows. Clair skips each test
            that needs the complete data, such as a row count test.
        sample_rows: The row limit of a sample, for each Trouve without a
            ``sample_sql``.
        threads: The number of Trouves that clair tests at one time. None takes
            the thread count of the environment.
        adapter: A connected warehouse adapter. The default makes a
            SnowflakeAdapter, connects it, and closes it at the end. A parallel
            test run opens one more connection for each other thread.

    Returns:
        A TestSummary with one TestResult for each test.

    Raises:
        ClairError: If discovery, routing, or the connection fails.
        ValueError: If *sample_rows* is less than 1.
    """
    _require_a_positive_sample_rows(sample_rows)

    # `environment` names the object, and `env` names the argument.
    environment = env
    # The caller replaces the thread count of the environment.
    thread_count = threads if threads is not None else environment.threads
    project_root, env_name, _, dag = _discover(project_dir, environment)

    # Keep each SOURCE, thus a selector can match a SOURCE. run_tests skips it.
    selected = _select_nodes(dag, select, exclude, keep_sources=True)
    if not selected:
        logger.info("test.no_trouves_selected")
        return TestSummary(results=[])

    warehouse_adapter = adapter
    owns_adapter = warehouse_adapter is None
    if warehouse_adapter is None:
        warehouse_adapter = SnowflakeAdapter()
        warehouse_adapter.connect(environment)

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
                dag,
                selected,
                warehouse_adapter,
                sample_scope=SampleScope.TEST_QUERIES if sample else SampleScope.NONE,
                sample_rows=sample_rows,
                threads=thread_count,
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
        warned=summary.warned_count,
        failed=summary.failed_count,
        errors=summary.error_count,
    )
    return summary


def validate(
    project_dir: str | Path | None = None,
    *,
    env: EnvSpec,
) -> ValidationReport:
    """Apply the project routing entries to each Trouve. This needs no connection.

    The report tells you the physical address problems, the collisions, and each
    address that an author wrote as text. A caller reads the lists, and it
    parses no text::

        report = clair.validate("~/projects/analytics", env="dev")
        for collision in report.collisions:
            print(collision.physical_address, collision.logical_addresses)

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
        env: An Environment, or an environment name. This function uses the
            name only, because it makes no connection.

    Returns:
        A ValidationReport. Read ``is_valid`` for the result of the command.

    Raises:
        ClairError: If clair cannot read the project or the routing file.
    """
    project_root = _resolve_project_root(project_dir)
    env_name = _env_name_of(env)
    return validate_project(project_root, env_name)


def clean(
    project_dir: str | Path | None = None,
    *,
    before: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> CleanOutput:
    """Remove the compiled artifacts of the old runs. This needs no connection.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
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
    project_root = _resolve_project_root(project_dir)
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
        cutoff=cutoff,
        runs=runs,
        dry_run=dry_run,
    )


def catalog(project_dir: str | Path | None = None) -> dict:
    """Make the documentation catalog of a project. This needs no connection.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.

    Returns:
        The catalog dictionary: one entry for each Trouve, and the lineage edges.

    Raises:
        ClairError: If discovery fails.
    """
    project_root = _resolve_project_root(project_dir)
    dag = build_dag(discover_project(project_root))
    return build_catalog(dag, project_root)


def docs(
    project_dir: str | Path | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8741,
    open_browser: bool = True,
) -> None:
    """Start the local documentation server. This function does not give control back.

    Args:
        project_dir: The root directory of the project. None starts the
            root search: clair walks up from the working directory to the
            first ``__routing__.py``.
        host: The address of the server.
        port: The port of the server.
        open_browser: If True, clair opens the browser on the server address.

    Raises:
        ClairError: If discovery fails.
        OSError: If the port is in use.
    """
    project_root = _resolve_project_root(project_dir)
    project_catalog = catalog(project_root)
    logger.info(
        "docs.start",
        project=str(project_root),
        host=host,
        port=port,
    )
    serve(project_catalog, host=host, port=port, open_browser=open_browser)


__all__ = [
    "RunHandle",
    "catalog",
    "clean",
    "compile",
    "docs",
    "run",
    "start_run",
    "test",
    "validate",
]
