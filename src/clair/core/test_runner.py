"""Test runner -- execute data quality tests against Snowflake."""

from __future__ import annotations

import re

import structlog
from pydantic import BaseModel

from clair.adapters.base import WarehouseAdapter
from clair.core.dag import ClairDag
from clair.trouves.trouve import TrouveType


logger = structlog.get_logger()



class TestResult(BaseModel):
    """Result of running a single data quality test.

    Attributes:
        full_name: Fully-qualified Snowflake object name of the tested Trouve.
        test_index: Zero-based index of this test within the Trouve's test list.
        test_type: Human-readable label (e.g. "unique", "not_null", "sql").
        column_name: Column under test, or None for SQL-level tests.
        passed: True if the test query returned zero rows (no violations).
        failing_row_count: Number of rows violating the test condition.
        query_id: Warehouse query ID.
        query_url: URL to the query in the Snowflake console.
        error: Error message if the query itself failed to execute.
    """

    full_name: str
    test_index: int
    test_type: str
    column_name: str | None
    passed: bool
    failing_row_count: int
    query_id: str | None = None
    query_url: str | None = None
    error: str | None = None


class TestSummary(BaseModel):
    """Structured result of a test run."""

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
        """Produce the formatted summary string for stdout."""
        total = len(self.results)
        lines = [
            "=== Clair Test ===",
            "",
            f"Running {total} test{'s' if total != 1 else ''}...",
            "",
        ]

        passed_count = 0
        failed_count = 0
        error_count = 0

        for i, r in enumerate(self.results, 1):
            label = f"{r.full_name} :: {r.test_type}"
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
) -> list[TestResult]:
    """Execute data quality tests for selected Trouves.

    Iterates through each selected Trouve in the DAG, generates test SQL
    for each Test declared on the Trouve, executes it, and collects results.
    SOURCE Trouves are skipped because they have no materialized table to
    test against.

    Args:
        dag: The project DAG.
        selected: Full_names of Trouves to test.
        adapter: Connected warehouse adapter.
        use_sample: When True, enable per-Trouve native sampling via
                    ``trouve.sample()`` and skip tests not meaningful on
                    sampled data (e.g. ``TestRowCount``).

    Returns:
        List of TestResult, one per test executed.
    """
    results: list[TestResult] = []

    for name in selected:
        trouve = dag.get_trouve(name)

        # Skip SOURCEs -- there is no clair-created table to test against
        if trouve.type == TrouveType.SOURCE:
            continue

        for test_index, test in enumerate(trouve.tests):
            # Derive column_name from column-level tests, None otherwise
            column_name = getattr(test, "column", None)

            assert trouve.compiled is not None
            routed_name = trouve.compiled.full_name

            # Skip tests that are meaningless on sampled data.
            if use_sample and not test.is_run_with_sample:
                logger.info(
                    "test.skipped_for_sample",
                    trouve=routed_name,
                    test_type=test.label,
                    reason="is_run_with_sample=False",
                )
                continue

            try:
                sql = test.to_sql(routed_name)

                if use_sample:
                    sample_subquery = trouve.sample()
                    pattern = re.compile(re.escape(f"FROM {routed_name}"), re.IGNORECASE)
                    sql = pattern.sub(f"FROM {sample_subquery}", sql)

                query_result = adapter.execute(sql)

                if not query_result.success:
                    logger.warning("test.query_error", trouve=name, test_type=test.label, column=column_name, error=query_result.error, query_id=query_result.query_id)
                    results.append(
                        TestResult(
                            full_name=routed_name,
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
                    logger.info("test.result", trouve=routed_name, test_type=test.label, column=column_name, passed=passed, failing_rows=query_result.row_count, query_id=query_result.query_id)
                    results.append(
                        TestResult(
                            full_name=routed_name,
                            test_index=test_index,
                            test_type=test.label,
                            column_name=column_name,
                            passed=passed,
                            failing_row_count=query_result.row_count,
                            query_id=query_result.query_id,
                            query_url=query_result.query_url,
                        )
                    )
            except Exception as e:
                logger.warning("test.exception", trouve=routed_name, test_type=test.label, column=column_name, error=str(e))
                results.append(
                    TestResult(
                        full_name=routed_name,
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
    """Build a structured TestSummary from test results.

    Args:
        results: List of TestResult objects.

    Returns:
        A TestSummary with structured data and a .render() method.
    """
    return TestSummary(results=results)
