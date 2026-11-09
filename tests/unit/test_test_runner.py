"""The tests of the test runner, which executes the data quality tests."""

from __future__ import annotations

import pytest

from clair.adapters.base import StatementStatus
from clair.core.dag import build_dag
from clair.core.test_runner import (
    SampleScope,
    TestSummary,
    run_tests,
)
from clair.trouves._refs import THIS_PLACEHOLDER
from clair.trouves.test import (
    THIS,
    TestNotNull,
    TestRowCount,
    TestSql,
    TestUnique,
    TestUniqueColumns,
)
from clair.trouves.trouve import TrouveAbc
from clair.trouves.trouve_type import TrouveType
from tests.helpers import RecordingAdapter, compile_trouve, make_test_result, make_trouve


def _make_trouve_with_tests(
    physical_address: str,
    trouve_type: TrouveType,
    tests: list,
    imports: list[str] | None = None,
    sql: str = "SELECT 1",
) -> TrouveAbc:
    """Make a compiled Trouve that holds some tests."""
    raw_sql = "" if trouve_type == TrouveType.SOURCE else sql
    return compile_trouve(
        make_trouve(trouve_type, raw_sql, tests=tests), physical_address, imports=imports
    )



class TestRunTests:
    def test_passing_test_row_count_zero(self):
        """A test passes when the query gives zero rows."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].failing_row_count == 0
        assert results[0].statement.query_id is not None

    def test_failing_test_row_count_positive(self):
        """A test fails when the query gives one row or more."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter(select_row_count=3)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].failing_row_count == 3

    def test_source_trouves_are_skipped(self):
        """Clair skips each SOURCE Trouve, and a test on it too."""
        dt = _make_trouve_with_tests(
            "db.s.raw_orders",
            TrouveType.SOURCE,
            [TestNotNull(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.raw_orders"], adapter)

        assert len(results) == 0
        assert adapter.record.statements == []

    def test_multiple_tests_on_one_trouve(self):
        """Clair executes each test on one Trouve."""
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
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 3
        assert all(r.passed for r in results)
        assert results[0].test_index == 0
        assert results[1].test_index == 1
        assert results[2].test_index == 2

    def test_adapter_query_failure_produces_error_result(self):
        """When the adapter gives success=False, the test result holds the error."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter(fail_on=[""])

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].error is not None


class TestFormatTestOutput:
    def test_format_includes_structured_counts(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
            make_test_result(
                "db.s.t",
                test_type="not_null",
                column_name="email",
                test_index=1,
                row_count=5,
                query_id="qid-2",
                query_url="https://sf/#/qid-2",
            ),
        ]

        output = TestSummary(results=results)

        assert output.passed_count == 1
        assert output.failed_count == 1
        assert output.error_count == 0
        assert len(output.results) == 2

    def test_format_empty_results(self):
        output = TestSummary(results=[])
        assert output.passed_count == 0
        assert output.failed_count == 0
        assert output.error_count == 0
        assert output.results == []

    def test_format_all_passed(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
            make_test_result(
                "db.s.t",
                test_type="not_null",
                column_name="email",
                test_index=1,
                query_id="qid-2",
                query_url="https://sf/#/qid-2",
            ),
        ]
        output = TestSummary(results=results)
        assert output.passed_count == 2
        assert output.failed_count == 0
        assert output.error_count == 0

    def test_format_all_failed(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                row_count=5,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
        ]
        output = TestSummary(results=results)
        assert output.passed_count == 0
        assert output.failed_count == 1
        assert output.error_count == 0

    def test_format_with_errors(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                status=StatementStatus.FAILURE,
                error="Query execution failed",
            ),
        ]
        output = TestSummary(results=results)
        assert output.passed_count == 0
        assert output.failed_count == 0
        assert output.error_count == 1

    def test_format_mixed_pass_fail_error(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
            make_test_result(
                "db.s.t",
                test_type="not_null",
                column_name="email",
                test_index=1,
                row_count=3,
                query_id="qid-2",
                query_url="https://sf/#/qid-2",
            ),
            make_test_result(
                "db.s.t",
                test_type="sql",
                column_name=None,
                test_index=2,
                status=StatementStatus.FAILURE,
                error="Syntax error",
            ),
        ]
        output = TestSummary(results=results)
        assert output.passed_count == 1
        assert output.failed_count == 1
        assert output.error_count == 1

    def test_format_list_properties(self):
        results = [
            make_test_result(
                "db.s.t",
                test_type="unique",
                column_name="id",
                test_index=0,
                query_id="qid-1",
                query_url="https://sf/#/qid-1",
            ),
            make_test_result(
                "db.s.t",
                test_type="not_null",
                column_name="email",
                test_index=1,
                row_count=3,
                query_id="qid-2",
                query_url="https://sf/#/qid-2",
            ),
            make_test_result(
                "db.s.t",
                test_type="sql",
                column_name=None,
                test_index=2,
                status=StatementStatus.FAILURE,
                error="Syntax error",
            ),
        ]
        output = TestSummary(results=results)
        assert len(output.passed_results) == 1
        assert output.passed_results[0].test_type == "unique"
        assert len(output.failed_results) == 1
        assert output.failed_results[0].test_type == "not_null"
        assert len(output.error_results) == 1
        assert output.error_results[0].test_type == "sql"

    def test_is_test_summary_instance(self):
        from clair.core.test_runner import TestSummary

        output = TestSummary(results=[])
        assert isinstance(output, TestSummary)


class TestRunTestsEdgeCases:
    def test_trouve_with_no_tests_produces_no_results(self):
        dt = _make_trouve_with_tests("db.s.orders", TrouveType.TABLE, [])
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 0
        assert adapter.record.statements == []

    def test_nonexistent_trouve_in_selected_raises(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        with pytest.raises(KeyError):
            run_tests(dag, ["db.s.nonexistent"], adapter)

    def test_test_result_fields_populated(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestUnique(column="id")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter)

        r = results[0]
        assert str(r.address) == "db.s.orders"
        assert r.test_index == 0
        assert r.test_type == "unique"
        assert r.column_name == "id"
        assert r.error == ""

    def test_not_null_test_type_label(self):
        dt = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE,
            [TestNotNull(column="email")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

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
        adapter = RecordingAdapter()

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
        adapter = RecordingAdapter(select_row_count=1)

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
        adapter = RecordingAdapter()

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
        adapter = RecordingAdapter(select_row_count=2)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False


# The scopes that sample the data. Each one skips the tests that need the
# complete data.
SAMPLED_SCOPES = [SampleScope.TEST_QUERIES, SampleScope.RUN_INPUTS]


class TestSampleScope:
    @pytest.mark.parametrize("sample_scope", SAMPLED_SCOPES)
    def test_a_sampled_scope_skips_each_test_that_needs_the_complete_data(
        self, sample_scope: SampleScope
    ):
        """TestRowCount and TestSql need each row. The column tests run on a sample."""
        trouve = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [
                TestUnique(column="id"),
                TestRowCount(min_rows=1),
                TestNotNull(column="id"),
                TestSql(sql="SELECT 1", resolved_sql="SELECT 1"),
            ],
        )
        dag = build_dag([trouve])

        results = run_tests(dag, ["db.s.orders"], RecordingAdapter(), sample_scope=sample_scope)

        assert [result.test_type for result in results] == ["unique", "not_null"]

    @pytest.mark.parametrize("sample_scope", SAMPLED_SCOPES)
    def test_a_sampled_scope_runs_no_query_for_a_skipped_test(self, sample_scope: SampleScope):
        trouve = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE, [TestRowCount(min_rows=1)]
        )
        dag = build_dag([trouve])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter, sample_scope=sample_scope)

        assert results == []
        assert adapter.record.statements == []

    def test_no_scope_runs_each_test(self):
        trouve = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE, [TestRowCount(min_rows=1)]
        )
        dag = build_dag([trouve])

        results = run_tests(dag, ["db.s.orders"], RecordingAdapter())

        assert len(results) == 1

    @pytest.mark.parametrize(
        ("sample_scope", "reads_the_sample"),
        [
            (SampleScope.NONE, False),
            (SampleScope.TEST_QUERIES, True),
            # The run built the object from a sample. The test reads all of it.
            (SampleScope.RUN_INPUTS, False),
        ],
    )
    def test_only_the_test_query_scope_reads_the_sample(
        self, sample_scope: SampleScope, reads_the_sample: bool
    ):
        trouve = _make_trouve_with_tests(
            "db.s.orders", TrouveType.TABLE, [TestUnique(column="id")]
        )
        dag = build_dag([trouve])
        adapter = RecordingAdapter()

        run_tests(dag, ["db.s.orders"], adapter, sample_scope=sample_scope, sample_rows=10)

        assert ("LIMIT 10" in adapter.record.statements[-1]) is reads_the_sample


class TestTestSql:
    def test_to_sql_gives_the_resolved_sql(self):
        """to_sql gives resolved_sql. Discovery replaces each token in sql."""
        pre_resolved = "SELECT * FROM db.schema.orders WHERE amount < 0"
        test = TestSql(sql=pre_resolved, resolved_sql=pre_resolved)
        assert test.to_sql("db.schema.orders") == pre_resolved

    def test_label_is_sql(self):
        test = TestSql(sql="SELECT 1", resolved_sql="SELECT 1")
        assert test.label == "sql"

    def test_is_run_with_sample_false(self):
        test = TestSql(sql="SELECT 1", resolved_sql="SELECT 1")
        assert test.is_run_with_sample is False

    def test_this_sentinel_format_returns_placeholder(self):
        assert f"{THIS}" == THIS_PLACEHOLDER

    def test_passes_when_zero_rows(self):
        # The SQL holds the true names already, as after discovery.
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0", resolved_sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].test_type == "sql"
        assert results[0].column_name is None

    def test_fails_when_nonzero_rows(self):
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0", resolved_sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter(select_row_count=4)

        results = run_tests(dag, ["db.s.orders"], adapter)

        assert len(results) == 1
        assert results[0].passed is False
        assert results[0].failing_row_count == 4

    def test_sql_sent_to_adapter_verbatim(self):
        """The adapter executes resolved_sql exactly. Clair changes nothing at run time."""
        pre_resolved = "SELECT * FROM db.s.orders WHERE amount < 0"
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql=pre_resolved, resolved_sql=pre_resolved)],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        run_tests(dag, ["db.s.orders"], adapter)

        executed_sql = adapter.record.statements[-1]
        assert executed_sql == pre_resolved

    def test_skipped_for_source_trouve(self):
        dt = _make_trouve_with_tests(
            "db.s.raw",
            TrouveType.SOURCE,
            [TestSql(sql="SELECT * FROM db.s.raw WHERE 1=0", resolved_sql="SELECT * FROM db.s.raw WHERE 1=0")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.raw"], adapter)

        assert len(results) == 0
        assert adapter.record.statements == []

    def test_skipped_on_a_sample(self):
        """Clair skips a TestSql on a sample, because is_run_with_sample is False."""
        dt = _make_trouve_with_tests(
            "db.s.orders",
            TrouveType.TABLE,
            [TestSql(sql="SELECT * FROM db.s.orders WHERE amount < 0", resolved_sql="SELECT * FROM db.s.orders WHERE amount < 0")],
        )
        dag = build_dag([dt])
        adapter = RecordingAdapter()

        results = run_tests(dag, ["db.s.orders"], adapter, sample_scope=SampleScope.TEST_QUERIES)

        assert len(results) == 0
        assert adapter.record.statements == []
