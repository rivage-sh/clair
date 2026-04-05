"""Tests for the test runner (data quality tests)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from clair.adapters.base import QueryResult, WarehouseAdapter
from clair.core.dag import build_dag
from clair.core.test_runner import (
    TestResult,
    format_test_output,
    run_tests,
)
from clair.trouves.config import ResolvedConfig
from clair.trouves._refs import THIS_PLACEHOLDER
from clair.trouves.test import (
    TestNotNull,
    TestRowCount,
    TestSql,
    TestUnique,
    TestUniqueColumns,
    THIS,
)
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


def _make_trouve_with_tests(
    full_name: str,
    trouve_type: TrouveType,
    tests: list,
    imports: list[str] | None = None,
    sql: str = "SELECT 1",
) -> Trouve:
    """Helper to build a compiled Trouve with tests attached."""
    raw_sql = "" if trouve_type == TrouveType.SOURCE else sql
    t = Trouve(type=trouve_type, sql=raw_sql, tests=tests)
    t.compiled = CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql=raw_sql,
        file_path=Path(f"/fake/{full_name.replace('.', '/')}.py"),
        module_name=full_name,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return t


def _make_mock_adapter(row_count: int = 0, success: bool = True) -> WarehouseAdapter:
    """Create a mock adapter that returns a fixed row_count."""
    adapter = MagicMock(spec=WarehouseAdapter)
    call_counter = 0

    def mock_execute(sql: str) -> QueryResult:
        nonlocal call_counter
        call_counter += 1
        query_id = f"test-qid-{call_counter:04d}"
        return QueryResult(
            query_id=query_id,
            query_url=f"https://test.snowflake.com/#/query/{query_id}",
            success=success,
            row_count=row_count,
            error=None if success else "Simulated query failure",
        )

    adapter.execute.side_effect = mock_execute
    return adapter



class TestRunTests:
    def test_passing_test_row_count_zero(self):
        """A test passes when the query returns zero rows."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].failing_row_count == 0
        assert results[0].query_id is not None

    def test_failing_test_row_count_positive(self):
        """A test fails when the query returns one or more rows."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=3)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].failing_row_count == 3

    def test_source_trouves_are_skipped(self):
        """SOURCE Trouves should be skipped even if they have tests declared."""
        dt = _make_trouve_with_tests(
            "db.s.raw_orders",
            TrouveType.SOURCE,
            [TestNotNull(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.raw_orders"], adapter)

        assert len(results) == 0
        cast(Any, adapter.execute).assert_not_called()

    def test_multiple_tests_on_one_trouve(self):
        """Multiple tests on the same Trouve should all be executed."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [
                TestUnique(column="id"),
                TestNotNull(column="email"),
                TestRowCount(min_rows=1),
            ],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 3
        assert all(r.passed for r in results)
        assert results[0].test_index == 0
        assert results[1].test_index == 1
        assert results[2].test_index == 2

    def test_adapter_query_failure_produces_error_result(self):
        """When the adapter returns success=False, the test result records the error."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0, success=False)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].error is not None


class TestFormatTestOutput:
    def test_format_includes_structured_counts(self):
        results = [
            TestResult(
                full_name="db.s.t",
                test_index=0,
                test_type="unique",
                column_name="id",
                passed=True,
                failing_row_count=0,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
            TestResult(
                full_name="db.s.t",
                test_index=1,
                test_type="not_null",
                column_name="email",
                passed=False,
                failing_row_count=5,
                query_id="qid-2",
                query_url="https://sf/#/qid-2",
            ),
        ]

        output = format_test_output(results)

        assert output.passed_count == 1
        assert output.failed_count == 1
        assert output.error_count == 0
        assert len(output.results) == 2

    def test_format_empty_results(self):
        output = format_test_output([])
        assert output.passed_count == 0
        assert output.failed_count == 0
        assert output.error_count == 0
        assert output.results == []

    def test_format_all_passed(self):
        results = [
            TestResult(
                full_name="db.s.t", test_index=0, test_type="unique",
                column_name="id", passed=True, failing_row_count=0,
                query_id="qid-1", query_url="https://sf/#/qid-1",
            ),
            TestResult(
                full_name="db.s.t", test_index=1, test_type="not_null",
                column_name="email", passed=True, failing_row_count=0,
                query_id="qid-2", query_url="https://sf/#/qid-2",
            ),
        ]
        output = format_test_output(results)
        assert output.passed_count == 2
        assert output.failed_count == 0
        assert output.error_count == 0

    def test_format_all_failed(self):
        results = [
            TestResult(
                full_name="db.s.t", test_index=0, test_type="unique",
                column_name="id", passed=False, failing_row_count=5,
                query_id="qid-1", query_url="https://sf/#/qid-1",
            ),
        ]
        output = format_test_output(results)
        assert output.passed_count == 0
        assert output.failed_count == 1
        assert output.error_count == 0

    def test_format_with_errors(self):
        results = [
            TestResult(
                full_name="db.s.t", test_index=0, test_type="unique",
                column_name="id", passed=False, failing_row_count=0,
                error="Query execution failed",
            ),
        ]
        output = format_test_output(results)
        assert output.passed_count == 0
        assert output.failed_count == 0
        assert output.error_count == 1

    def test_format_mixed_pass_fail_error(self):
        results = [
            TestResult(
                full_name="db.s.t", test_index=0, test_type="unique",
                column_name="id", passed=True, failing_row_count=0,
                query_id="qid-1", query_url="https://sf/#/qid-1",
            ),
            TestResult(
                full_name="db.s.t", test_index=1, test_type="not_null",
                column_name="email", passed=False, failing_row_count=3,
                query_id="qid-2", query_url="https://sf/#/qid-2",
            ),
            TestResult(
                full_name="db.s.t", test_index=2, test_type="sql",
                column_name=None, passed=False, failing_row_count=0,
                error="Syntax error",
            ),
        ]
        output = format_test_output(results)
        assert output.passed_count == 1
        assert output.failed_count == 1
        assert output.error_count == 1

    def test_format_list_properties(self):
        results = [
            TestResult(
                full_name="db.s.t", test_index=0, test_type="unique",
                column_name="id", passed=True, failing_row_count=0,
                query_id="qid-1", query_url="https://sf/#/qid-1",
            ),
            TestResult(
                full_name="db.s.t", test_index=1, test_type="not_null",
                column_name="email", passed=False, failing_row_count=3,
                query_id="qid-2", query_url="https://sf/#/qid-2",
            ),
            TestResult(
                full_name="db.s.t", test_index=2, test_type="sql",
                column_name=None, passed=False, failing_row_count=0,
                error="Syntax error",
            ),
        ]
        output = format_test_output(results)
        assert len(output.passed_results) == 1
        assert output.passed_results[0].test_type == "unique"
        assert len(output.failed_results) == 1
        assert output.failed_results[0].test_type == "not_null"
        assert len(output.error_results) == 1
        assert output.error_results[0].test_type == "sql"

    def test_is_test_summary_instance(self):
        from clair.core.test_runner import TestSummary

        output = format_test_output([])
        assert isinstance(output, TestSummary)


class TestRunTestsEdgeCases:
    def test_trouve_with_no_tests_produces_no_results(self):
        dt = _make_trouve_with_tests("db.s.orders", TrouveType.TABLE, [])
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 0
        cast(Any, adapter.execute).assert_not_called()

    def test_nonexistent_trouve_in_selected_raises(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        with pytest.raises(Exception):
            run_tests(dag, ["db.s.nonexistent"], adapter)

    def test_test_result_fields_populated(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        r = results[0]
        assert r.full_name == "db.s.orders"
        assert r.test_index == 0
        assert r.test_type == "unique"
        assert r.column_name == "id"
        assert r.error is None

    def test_not_null_test_type_label(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestNotNull(column="email")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)
        assert results[0].test_type == "not_null"


class TestRunTestsRowCount:
    def test_row_count_passing(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestRowCount(min_rows=1)],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].column_name is None
        assert results[0].test_type == "row_count"

    def test_row_count_failing(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestRowCount(min_rows=1)],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=1)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False


class TestRunTestsUniqueColumns:
    def test_unique_columns_passing(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUniqueColumns(columns=["a", "b"])],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].test_type == "unique_columns"

    def test_unique_columns_failing(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUniqueColumns(columns=["a", "b"])],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=2)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False


class TestUseSample:
    def test_row_count_skipped_when_use_sample(self):
        """TestRowCount is skipped when use_sample=True."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestRowCount(min_rows=1)],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter, use_sample=True)

        assert len(results) == 0
        cast(Any, adapter.execute).assert_not_called()

    def test_non_row_count_tests_run_with_use_sample(self):
        """Non-row-count tests run when use_sample=True."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter, use_sample=True)

        assert len(results) == 1
        assert results[0].passed is True

    def test_use_sample_applies_top_1000(self):
        """When use_sample=True, trouve.sample() wraps the table with TOP 1000."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        run_tests(dag, ["db.s.orders"], adapter, use_sample=True)

        executed_sql = cast(Any, adapter.execute).call_args[0][0]
        assert "SELECT TOP 1000" in executed_sql

    def test_mixed_tests_use_sample_skips_row_count(self):
        """With use_sample=True and mixed tests, row_count is skipped but others run."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [
                TestUnique(column="id"),
                TestRowCount(min_rows=1),
                TestNotNull(column="id"),
            ],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter, use_sample=True)

        # Only 2 results (unique + not_null), row_count skipped
        assert len(results) == 2
        assert all(r.test_type != "row_count" for r in results)

    def test_use_sample_false_runs_all(self):
        """With use_sample=False, all tests run without sampling."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestRowCount(min_rows=1)],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        executed_sql = cast(Any, adapter.execute).call_args[0][0]
        assert "TOP" not in executed_sql


class TestTestSql:
    def test_to_sql_is_passthrough(self):
        """to_sql returns self.sql unchanged — discovery is responsible for all token resolution."""
        pre_resolved = "SELECT * FROM db.schema.orders WHERE amount < 0"
        test = TestSql(sql=pre_resolved)
        assert test.to_sql("db.schema.orders") == pre_resolved

    def test_label_is_sql(self):
        test = TestSql(sql="SELECT 1")
        assert test.label == "sql"

    def test_is_run_with_sample_false(self):
        test = TestSql(sql="SELECT 1")
        assert test.is_run_with_sample is False

    def test_this_sentinel_format_returns_placeholder(self):
        assert f"{THIS}" == THIS_PLACEHOLDER

    def test_passes_when_zero_rows(self):
        # SQL is pre-resolved as it would be after discovery
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].test_type == "sql"
        assert results[0].column_name is None

    def test_fails_when_nonzero_rows(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=4)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].failing_row_count == 4

    def test_sql_sent_to_adapter_verbatim(self):
        """The SQL executed on the adapter is exactly test.sql — no runtime substitution."""
        pre_resolved = "SELECT * FROM db.s.orders WHERE amount < 0"
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql=pre_resolved)],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        run_tests(dag, ["db.s.orders"], adapter)

        executed_sql = cast(Any, adapter.execute).call_args[0][0]
        assert executed_sql == pre_resolved

    def test_skipped_for_source_trouve(self):
        dt = _make_trouve_with_tests(
            "db.s.raw",
            TrouveType.SOURCE,
            [TestSql(sql="SELECT * FROM db.s.raw WHERE 1=0")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.raw"], adapter)

        assert len(results) == 0
        cast(Any, adapter.execute).assert_not_called()

    def test_skipped_when_use_sample(self):
        """TestSql is skipped when use_sample=True (is_run_with_sample=False)."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = _make_mock_adapter(row_count=0)

        results = run_tests(dag, ["db.s.orders"], adapter, use_sample=True)

        assert len(results) == 0
        cast(Any, adapter.execute).assert_not_called()
