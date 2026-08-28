"""The test runner. It executes the data quality tests on Snowflake."""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

import structlog
from pydantic import BaseModel, computed_field

from clair.adapters.base import Statement, StatementStatus, WarehouseAdapter
from clair.adapters.pool import AdapterPool
from clair.core.dag import ClairDag
from clair.trouves.address import TrouveAddress
from clair.trouves.trouve import TrouveType

logger = structlog.get_logger()



class TestResult(BaseModel):
    """The result of one data quality test.

    The test query gives the rows that disobey the test condition. Thus the
    statement holds the count, the query ID and the error, and this object needs
    no copy of them.

    Attributes:
        address: The address of the Trouve that the test examines.
        test_index: The index of this test in the test list of the Trouve. The
            first index is 0.
        test_type: A label for a person to read, such as "unique", "not_null",
            or "sql".
        column_name: The column that the test examines. It is None for a test
            on the full table.
        statement: The test query, and what the warehouse answered.
    """

    address: TrouveAddress
    test_index: int
    test_type: str
    column_name: str | None
    statement: Statement

    @computed_field  # type: ignore[prop-decorator]
    @property
    def passed(self) -> bool:
        """True if the test query gave zero rows. Thus the data is correct."""
        return self.statement.success and self.statement.row_count == 0

    @property
    def failing_row_count(self) -> int:
        """The number of rows that disobey the test condition."""
        return self.statement.row_count if self.statement.success else 0

    @property
    def error(self) -> str:
        """The error message if the query itself did not execute."""
        return self.statement.error


class TestSummary(BaseModel):
    """The result of one test run."""

    results: list[TestResult]

    @property
    def passed_results(self) -> list[TestResult]:
        return [r for r in self.results if r.passed]

    @property
    def failed_results(self) -> list[TestResult]:
        """Each test that the data did not obey. A query that failed is not here."""
        return [r for r in self.results if not r.passed and not r.error]

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
                lines.append(
                    f"[{i}/{total}] {label} ... FAIL ({r.failing_row_count} failing row{'s' if r.failing_row_count != 1 else ''})"
                )

            if r.statement.query_id:
                lines.append(f"      Query ID: {r.statement.query_id}")
            if r.statement.query_url:
                lines.append(f"      URL: {r.statement.query_url}")

            lines.append("")

        lines.append(
            f"=== Done: {self.passed_count} passed, {self.failed_count} failed, "
            f"{self.error_count} errors ==="
        )

        return "\n".join(lines)

def _run_trouve_tests(
    name: str,
    dag: ClairDag,
    adapter: WarehouseAdapter,
    use_sample: bool,
    query_addresses: dict[str, str],
) -> list[TestResult]:
    """Execute each data quality test of one Trouve.

    A thread of the test run calls this function. *adapter* is the private
    connection of that thread.

    Args:
        name: The name of the Trouve in the DAG.
        dag: The project DAG.
        adapter: The warehouse adapter of the thread that calls this function.
        use_sample: If True, the function takes a sample of the Trouve.
        query_addresses: The map of node name to the address to query.

    Returns:
        A list of TestResult, one item for each test. The list is empty for a
        SOURCE Trouve, because clair makes no table for a SOURCE Trouve.
    """
    results: list[TestResult] = []
    trouve = dag.get_trouve(name)

    # Skip each SOURCE. Clair makes no table for a SOURCE Trouve.
    if trouve.type == TrouveType.SOURCE:
        return results

    for test_index, test in enumerate(trouve.tests):
        # A column test supplies column_name. A different test gives None.
        column_name = getattr(test, "column", None)

        assert trouve.compiled is not None
        physical_address = str(trouve.compiled.physical_address)
        queried_address = query_addresses.get(name, physical_address)

        # Skip each test that needs the complete table.
        if use_sample and not test.is_run_with_sample:
            logger.info(
                "test.skipped_for_sample",
                trouve=physical_address,
                test_type=test.label,
                reason="is_run_with_sample=False",
            )
            continue

        sql = ""
        try:
            sql = test.to_sql(queried_address)

            if use_sample:
                # sample() gives the physical address of the Trouve. Point it
                # at the object that the test examines.
                sample_subquery = trouve.sample().replace(physical_address, queried_address)
                pattern = re.compile(re.escape(f"FROM {queried_address}"), re.IGNORECASE)
                sql = pattern.sub(f"FROM {sample_subquery}", sql)

            statement = adapter.execute(sql)

            if not statement.success:
                logger.warning("test.query_error", trouve=name, test_type=test.label, column=column_name, error=statement.error, query_id=statement.query_id)
            else:
                logger.info("test.result", trouve=physical_address, test_type=test.label, column=column_name, passed=statement.row_count == 0, failing_rows=statement.row_count, query_id=statement.query_id)
        except Exception as e:  # noqa: BLE001 — one test that fails must not stop the complete run
            logger.warning("test.exception", trouve=physical_address, test_type=test.label, column=column_name, error=str(e))
            statement = Statement(sql=sql, status=StatementStatus.FAILURE, error=str(e))

        results.append(
            TestResult(
                address=trouve.compiled.physical_address,
                test_index=test_index,
                test_type=test.label,
                column_name=column_name,
                statement=statement,
            )
        )

    return results


def run_tests(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    use_sample: bool = False,
    query_addresses: dict[str, str] | None = None,
    threads: int = 1,
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
        use_sample: If True, the function takes a sample of each Trouve with
            ``trouve.sample()``. It also skips each test that needs the
            complete table, such as ``TestRowCount``.
        query_addresses: An optional map of node name to the address to query.
            The runner gives the staging address here, so the tests examine the
            candidate before clair promotes it. Each result still reports the
            physical address, so the output does not change with the query address.
        threads: The number of Trouves that clair tests at one time, and thus
            the number of warehouse connections. The tests of one Trouve always
            run one after the other.

    Returns:
        A list of TestResult, one item for each test. The list keeps the order
        of *selected*, also with more than one thread.

    Raises:
        ValueError: If *threads* is less than 1.
    """
    if threads < 1:
        raise ValueError(f"The thread count must be 1 or more, but it is {threads}")

    query_addresses = query_addresses or {}

    if threads == 1:
        results: list[TestResult] = []
        for name in selected:
            results.extend(
                _run_trouve_tests(name, dag, adapter, use_sample, query_addresses)
            )
        return results

    pool = AdapterPool(adapter, threads)

    def test_one(name: str) -> list[TestResult]:
        """Test one Trouve on the connection of this thread."""
        return _run_trouve_tests(name, dag, pool.acquire(), use_sample, query_addresses)

    try:
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="clair-test") as executor:
            # map() keeps the order of selected, thus the output does not change
            # with the thread count.
            per_trouve_results = list(executor.map(test_one, selected))
    finally:
        pool.close()

    return [result for trouve_results in per_trouve_results for result in trouve_results]
