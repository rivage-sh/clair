"""The test runner. It executes the data quality tests on Snowflake."""

from __future__ import annotations

import re

import structlog
from pydantic import BaseModel

from clair.adapters.base import WarehouseAdapter
from clair.core.dag import ClairDag
from clair.trouves.trouve import TrouveType

logger = structlog.get_logger()



class TestResult(BaseModel):
    """The result of one data quality test.

    Attributes:
        physical_address: The full Snowflake object name of the Trouve.
        test_index: The index of this test in the test list of the Trouve. The
            first index is 0.
        test_type: A label for a person to read, such as "unique", "not_null",
            or "sql".
        column_name: The column that the test examines. It is None for a test
            on the full table.
        passed: True if the test query gave zero rows. Thus the data is correct.
        failing_row_count: The number of rows that disobey the test condition.
        query_id: The warehouse query ID.
        query_url: The URL of the query in the Snowflake console.
        error: The error message if the query itself did not execute.
    """

    physical_address: str
    test_index: int
    test_type: str
    column_name: str | None
    passed: bool
    failing_row_count: int
    query_id: str | None = None
    query_url: str | None = None
    error: str | None = None


class TestSummary(BaseModel):
    """The result of one test run."""

    results: list[TestResult]

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed and not r.error)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and not r.error)

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if r.error)

    @property
    def passed_results(self) -> list[TestResult]:
        return [r for r in self.results if r.passed and not r.error]

    @property
    def failed_results(self) -> list[TestResult]:
        return [r for r in self.results if not r.passed and not r.error]

    @property
    def error_results(self) -> list[TestResult]:
        return [r for r in self.results if r.error]

    def render(self) -> str:
        """Make the complete summary text for stdout."""
        total = len(self.results)
        lines = [
            "=== Clair Test ===",
            "",
            f"Clair runs {total} test{'s' if total != 1 else ''}...",
            "",
        ]

        passed_count = 0
        failed_count = 0
        error_count = 0

        for i, r in enumerate(self.results, 1):
            label = f"{r.physical_address} :: {r.test_type}"
            if r.column_name:
                label += f" ({r.column_name})"

            if r.error:
                error_count += 1
                lines.append(f"[{i}/{total}] {label} ... ERROR")
                lines.append(f"      Error: {r.error}")
            elif r.passed:
                passed_count += 1
                lines.append(f"[{i}/{total}] {label} ... PASS")
            else:
                failed_count += 1
                lines.append(
                    f"[{i}/{total}] {label} ... FAIL ({r.failing_row_count} failing row{'s' if r.failing_row_count != 1 else ''})"
                )

            if r.query_id:
                lines.append(f"      Query ID: {r.query_id}")
            if r.query_url:
                lines.append(f"      URL: {r.query_url}")

            lines.append("")

        lines.append(
            f"=== Done: {passed_count} passed, {failed_count} failed, {error_count} errors ==="
        )

        return "\n".join(lines)


def run_tests(
    dag: ClairDag,
    selected: list[str],
    adapter: WarehouseAdapter,
    use_sample: bool = False,
    query_addresses: dict[str, str] | None = None,
) -> list[TestResult]:
    """Execute the data quality tests of the selected Trouves.

    The function reads each selected Trouve in the DAG. For each Test on the
    Trouve, it makes the test SQL, executes the SQL, and keeps the result. The
    function skips each SOURCE Trouve, because clair makes no table for a
    SOURCE Trouve.

    Args:
        dag: The project DAG.
        selected: The names of the Trouves to test.
        adapter: A warehouse adapter with an open connection.
        use_sample: If True, the function takes a sample of each Trouve with
            ``trouve.sample()``. It also skips each test that needs the
            complete table, such as ``TestRowCount``.
        query_addresses: An optional map of node name to the address to query.
            The runner gives the staging address here, so the tests examine the
            candidate before clair promotes it. Each result still reports the
            physical address, so the output does not change with the query address.

    Returns:
        A list of TestResult, one item for each test.
    """
    results: list[TestResult] = []
    query_addresses = query_addresses or {}

    for name in selected:
        trouve = dag.get_trouve(name)

        # Skip each SOURCE. Clair makes no table for a SOURCE Trouve.
        if trouve.type == TrouveType.SOURCE:
            continue

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

            try:
                sql = test.to_sql(queried_address)

                if use_sample:
                    # sample() gives the physical address of the Trouve. Point it
                    # at the object that the test examines.
                    sample_subquery = trouve.sample().replace(physical_address, queried_address)
                    pattern = re.compile(re.escape(f"FROM {queried_address}"), re.IGNORECASE)
                    sql = pattern.sub(f"FROM {sample_subquery}", sql)

                query_result = adapter.execute(sql)

                if not query_result.success:
                    logger.warning("test.query_error", trouve=name, test_type=test.label, column=column_name, error=query_result.error, query_id=query_result.query_id)
                    results.append(
                        TestResult(
                            physical_address=physical_address,
                            test_index=test_index,
                            test_type=test.label,
                            column_name=column_name,
                            passed=False,
                            failing_row_count=0,
                            query_id=query_result.query_id,
                            query_url=query_result.query_url,
                            error=query_result.error,
                        )
                    )
                else:
                    passed = query_result.row_count == 0
                    logger.info("test.result", trouve=physical_address, test_type=test.label, column=column_name, passed=passed, failing_rows=query_result.row_count, query_id=query_result.query_id)
                    results.append(
                        TestResult(
                            physical_address=physical_address,
                            test_index=test_index,
                            test_type=test.label,
                            column_name=column_name,
                            passed=passed,
                            failing_row_count=query_result.row_count,
                            query_id=query_result.query_id,
                            query_url=query_result.query_url,
                        )
                    )
            except Exception as e:  # noqa: BLE001 — one test that fails must not stop the complete run
                logger.warning("test.exception", trouve=physical_address, test_type=test.label, column=column_name, error=str(e))
                results.append(
                    TestResult(
                        physical_address=physical_address,
                        test_index=test_index,
                        test_type=test.label,
                        column_name=column_name,
                        passed=False,
                        failing_row_count=0,
                        error=str(e),
                    )
                )

    return results


def format_test_output(results: list[TestResult]) -> TestSummary:
    """Make a TestSummary from the test results.

    Args:
        results: A list of TestResult objects.

    Returns:
        A TestSummary. It holds the data and supplies a .render() method.
    """
    return TestSummary(results=results)
