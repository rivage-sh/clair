"""The test runner. It executes the data quality tests on Snowflake."""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum

import structlog
from pydantic import BaseModel

from clair.adapters.base import WarehouseAdapter
from clair.adapters.objects import Statement, StatementStatus
from clair.adapters.pool import AdapterPool
from clair.core.dag import ClairDag
from clair.core.references import render_sample
from clair.phase import Phase
from clair.trouves.address import TrouveAddress
from clair.trouves.plan import PlanContext, TestQuery, TestResult
from clair.trouves.run_config import RunMode
from clair.trouves.test import TestSeverity
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS, MaterializedTrouve, TrouveAbc
from clair.trouves.trouve_type import TrouveType

logger = structlog.get_logger()


class SampleScope(StrEnum):
    """The part of the data that a test run sees."""

    # Each test reads each row of the object.
    NONE = "none"
    # `clair test --sample`: each test reads ``Trouve.sample()`` of the object,
    # and clair skips each test that needs the complete data.
    TEST_QUERIES = "test_queries"
    # `clair run --sample`: the run built the object from a sample of each
    # input. Each test reads the complete object, and clair skips each test
    # that needs the complete data.
    RUN_INPUTS = "run_inputs"


class TestSummary(BaseModel):
    """The result of one test run."""

    results: list[TestResult]

    @property
    def passed_results(self) -> list[TestResult]:
        return [r for r in self.results if r.passed]

    @property
    def failed_results(self) -> list[TestResult]:
        """Each ERROR test that the data did not obey. A query that failed is not here."""
        return [
            result
            for result in self.results
            if not result.passed and not result.error and result.severity == TestSeverity.ERROR
        ]

    @property
    def warned_results(self) -> list[TestResult]:
        """Each WARN test that the data did not obey. These do not fail the command."""
        return [
            result
            for result in self.results
            if not result.passed and not result.error and result.severity == TestSeverity.WARN
        ]

    @property
    def blocking_results(self) -> list[TestResult]:
        """Each result that fails the command: a failed ERROR test, or a query that failed."""
        return [result for result in self.results if result.is_blocking]

    @property
    def error_results(self) -> list[TestResult]:
        """Each test whose query did not execute."""
        return [r for r in self.results if r.error]

    @property
    def passed_count(self) -> int:
        return len(self.passed_results)

    @property
    def failed_count(self) -> int:
        return len(self.failed_results)

    @property
    def warned_count(self) -> int:
        return len(self.warned_results)

    @property
    def error_count(self) -> int:
        return len(self.error_results)

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        total = len(self.results)
        lines = [
            "=== Clair Test ===",
            "",
            f"Clair runs {total} test{'s' if total != 1 else ''}...",
            "",
        ]

        for i, r in enumerate(self.results, 1):
            label = f"{r.address} :: {r.test_type}"
            if r.column_name:
                label += f" ({r.column_name})"

            if r.error:
                lines.append(f"[{i}/{total}] {label} ... ERROR")
                lines.append(f"      Error: {r.error}")
            elif r.passed:
                lines.append(f"[{i}/{total}] {label} ... PASS")
            else:
                outcome = "WARN" if r.severity == TestSeverity.WARN else "FAIL"
                lines.append(
                    f"[{i}/{total}] {label} ... {outcome} ({r.failing_row_count} failing row{'s' if r.failing_row_count != 1 else ''})"
                )

            if r.statement.query_id:
                lines.append(f"      Query ID: {r.statement.query_id}")
            if r.statement.query_url:
                lines.append(f"      URL: {r.statement.query_url}")

            lines.append("")

        lines.append(
            f"=== Done: {self.passed_count} passed, {self.warned_count} warned, "
            f"{self.failed_count} failed, {self.error_count} errors ==="
        )

        return "\n".join(lines)

def run_test_queries(
    trouve: TrouveAbc,
    queries: list[TestQuery],
    adapter: WarehouseAdapter,
    sample_scope: SampleScope = SampleScope.NONE,
    sample_subquery: str | None = None,
    queried_address: str | None = None,
) -> list[TestResult]:
    """Execute the test queries of one Trouve, and give one result for each query.

    The runner gives the queries of the test phase of the plan. Each result
    reports the physical address of the Trouve, so the output does not change
    with the address that the queries read.

    Args:
        trouve: The Trouve that the queries test.
        queries: The test queries, from ``Trouve.test()``.
        adapter: The warehouse adapter of the thread that calls this function.
        sample_scope: The part of the data that the tests see. Each scope
            except NONE skips each query with ``run_with_sample`` False.
        sample_subquery: The subquery that each query reads in place of
            *queried_address*, or None to read the object.
        queried_address: The address that the queries read. Clair replaces
            ``FROM <queried_address>`` with the sample subquery.
    """
    physical_address = trouve.require_compiled().physical_address
    results: list[TestResult] = []
    for query in queries:
        # Skip each test that needs the complete data.
        if sample_scope != SampleScope.NONE and not query.run_with_sample:
            logger.info(
                "test.skipped_for_sample",
                trouve=str(physical_address),
                test_type=query.label,
                reason="is_run_with_sample=False",
            )
            continue
        sql = query.sql
        try:
            if sample_subquery is not None and queried_address is not None:
                pattern = re.compile(re.escape(f"FROM {queried_address}"), re.IGNORECASE)
                # A function as the replacement keeps a backslash of a
                # sample_sql as text, and not as a group reference.
                sql = pattern.sub(lambda _match: f"FROM {sample_subquery}", sql)
            statement = adapter.execute(sql)
            if not statement.success:
                logger.warning("test.query_error", trouve=str(physical_address), test_type=query.label, column=query.column_name, error=statement.error, query_id=statement.query_id)
            else:
                logger.info("test.result", trouve=str(physical_address), test_type=query.label, column=query.column_name, passed=statement.row_count == 0, failing_rows=statement.row_count, query_id=statement.query_id)
        except Exception as e:  # noqa: BLE001 — one test that fails must not stop the complete run
            logger.warning("test.exception", trouve=str(physical_address), test_type=query.label, column=query.column_name, error=str(e))
            statement = Statement(sql=sql, status=StatementStatus.FAILURE, error=str(e))
        results.append(
            TestResult(
                address=physical_address,
                test_index=query.test_index,
                test_type=query.label,
                column_name=query.column_name,
                statement=statement.model_copy(update={"phase": Phase.TEST}),
                severity=query.severity,
            )
        )
    return results


def test_queries_of(trouve: MaterializedTrouve, address: TrouveAddress) -> list[TestQuery]:
    """Give the test queries of a Trouve against the object at *address*, as ``clair test`` runs them.

    ``clair test`` reads an object that exists, and makes no plan of a run.
    Thus the context holds the address only.
    """
    context = PlanContext(
        run_id="",
        effective_mode=RunMode.FULL_REFRESH,
        staged=False,
        offline=True,
        physical_address=address,
    )
    return trouve.test(context)


def _failed_results(trouve: MaterializedTrouve, error: str) -> list[TestResult]:
    """Give one result with *error* for each test of a Trouve whose tests clair cannot send."""
    logger.warning("test.exception", trouve=str(trouve.require_compiled().physical_address), error=error)
    return [
        TestResult(
            address=trouve.require_compiled().physical_address,
            test_index=test_index,
            test_type=test.label,
            column_name=getattr(test, "column", None),
            statement=Statement(status=StatementStatus.FAILURE, error=error),
            severity=test.severity,
        )
        for test_index, test in enumerate(trouve.tests)
    ]


def _run_trouve_tests(
    name: str,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    sample_scope: SampleScope,
    sample_subquery: str | None,
    run_id: str,
    environment_name: str,
) -> list[TestResult]:
    """Execute each data quality test of one Trouve.

    A thread of the test run calls this function. *adapter* is the private
    connection of that thread. The tests run with the session of the Trouve:
    its role, its warehouse, its query tag and its timeout.

    Returns:
        A list of TestResult, one item for each test. The list is empty for a
        SOURCE Trouve, because clair makes no table for a SOURCE Trouve.
    """
    trouve = dag.get_trouve(name)
    if not isinstance(trouve, MaterializedTrouve) or not trouve.tests:
        return []
    compiled = trouve.require_compiled()
    queried_address = str(compiled.physical_address)
    try:
        queries = test_queries_of(trouve, compiled.physical_address)
    except Exception as plan_error:  # noqa: BLE001 — test() can be user code, and it can raise anything
        return _failed_results(
            trouve, f"{type(trouve).__name__}.test() raised {type(plan_error).__name__}: {plan_error}"
        )
    try:
        adapter.use_session_of(
            compiled.config,
            compiled.config.query_tag(run_id, environment_name, str(trouve.logical_address)),
        )
    except Exception as session_error:  # noqa: BLE001 — each adapter fault becomes a failed TestResult
        return _failed_results(trouve, f"Clair cannot set the session context: {session_error}")
    return run_test_queries(
        trouve, queries, adapter, sample_scope, sample_subquery, queried_address
    )


def _sample_subqueries(
    dag: ClairDag,
    selected: list[str],
    sample_rows: int,
) -> dict[str, str]:
    """Give the sample that the tests of each selected Trouve read, by node name.

    A reference in a ``sample_sql`` reads the physical address of that Trouve,
    because a test run examines the objects that are in the warehouse.
    """
    physical_addresses = {
        id(trouve): trouve.compiled.physical_address
        for trouve in dag.trouves
        if trouve.compiled is not None
    }
    sample_subqueries: dict[str, str] = {}
    for name in selected:
        trouve = dag.get_trouve(name)
        if trouve.compiled is None or trouve.type == TrouveType.SOURCE:
            continue
        sample_subqueries[name] = render_sample(
            trouve, physical_addresses, trouve.compiled.physical_address, sample_rows
        )
    return sample_subqueries


def run_tests(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    sample_scope: SampleScope = SampleScope.NONE,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    threads: int = 1,
    run_id: str = "",
    environment_name: str = "",
) -> list[TestResult]:
    """Execute the data quality tests of the selected Trouves.

    The function reads each selected Trouve in the DAG. For each Test on the
    Trouve, it makes the test SQL, executes the SQL, and keeps the result. The
    function skips each SOURCE Trouve, because clair makes no table for a
    SOURCE Trouve.

    Args:
        dag: The project DAG.
        selected: The names of the Trouves to test.
        adapter: A warehouse adapter with an open connection. With more than one
            thread, clair uses it as the first connection of the pool, and it
            opens `threads - 1` more.
        sample_scope: The part of the data that the tests see. See
            ``SampleScope``. Each scope except NONE skips each test that needs
            the complete data, such as ``TestRowCount``.
        sample_rows: The row limit of ``Trouve.sample()`` in the TEST_QUERIES
            scope, for each Trouve without a ``sample_sql``.
        threads: The number of Trouves that clair tests at one time, and thus
            the number of warehouse connections. The tests of one Trouve always
            run one after the other.
        run_id: The identifier of this test run. The query tag holds it.
        environment_name: The environment of the test run. The query tag holds it.

    Returns:
        A list of TestResult, one item for each test. The list keeps the order
        of *selected*, also with more than one thread.

    Raises:
        ValueError: If *threads* is less than 1.
    """
    if threads < 1:
        raise ValueError(f"The thread count must be 1 or more, but it is {threads}")

    sample_subqueries = (
        _sample_subqueries(dag, selected, sample_rows)
        if sample_scope == SampleScope.TEST_QUERIES
        else {}
    )

    # One thread gives a pool of one: the adapter of the caller, and no new connection.
    pool = AdapterPool(adapter, threads)

    def test_one(name: str) -> list[TestResult]:
        """Test one Trouve on the connection of this thread."""
        return _run_trouve_tests(
            name,
            dag,
            pool.acquire(),
            sample_scope,
            sample_subqueries.get(name),
            run_id,
            environment_name,
        )

    try:
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="clair-test") as executor:
            # map() keeps the order of selected, thus the output does not change
            # with the thread count.
            per_trouve_results = list(executor.map(test_one, selected))
    finally:
        pool.close()

    return [result for trouve_results in per_trouve_results for result in trouve_results]
