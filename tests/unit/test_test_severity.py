"""A WARN test reports its rows, and the run continues. An ERROR test stops the promotion.

A test whose query fails stops the promotion with each severity: clair then
knows nothing about the data.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair import TestSeverity, TestUnique
from clair.adapters.base import StatementStatus
from clair.core.runner import RunFailure, RunSkip, RunSuccess
from clair.core.test_runner import TestResult, TestSummary
from clair.trouves.address import TrouveAddress
from tests.helpers import RecordingAdapter, make_environment, make_statement, write_project_marker

UPSTREAM = "db.derived.orders"
DOWNSTREAM = "db.derived.order_count"


def _result(severity: TestSeverity, failing_rows: int, query_error: str = "") -> TestResult:
    status = StatementStatus.FAILURE if query_error else StatementStatus.SUCCESS
    return TestResult(
        address=TrouveAddress.parse(UPSTREAM),
        test_index=0,
        test_type="unique",
        column_name="id",
        statement=make_statement(status=status, row_count=failing_rows, error=query_error),
        severity=severity,
    )


@pytest.mark.parametrize(
    ("severity", "failing_rows", "query_error", "is_blocking"),
    [
        (TestSeverity.ERROR, 0, "", False),
        (TestSeverity.WARN, 0, "", False),
        (TestSeverity.ERROR, 3, "", True),
        (TestSeverity.WARN, 3, "", False),
        (TestSeverity.ERROR, 0, "invalid identifier 'ID'", True),
        (TestSeverity.WARN, 0, "invalid identifier 'ID'", True),
    ],
)
def test_only_a_failed_error_test_or_a_failed_query_blocks(severity, failing_rows, query_error, is_blocking):
    assert _result(severity, failing_rows, query_error).is_blocking is is_blocking


def test_the_summary_counts_a_warning_apart_from_a_failure():
    summary = TestSummary(
        results=[
            _result(TestSeverity.ERROR, 0),
            _result(TestSeverity.WARN, 2),
            _result(TestSeverity.ERROR, 1),
            _result(TestSeverity.WARN, 0, "query failed"),
        ]
    )

    assert (summary.passed_count, summary.warned_count, summary.failed_count, summary.error_count) == (1, 1, 1, 1)
    assert len(summary.blocking_results) == 2


def test_a_test_has_the_error_severity_by_default():
    assert TestUnique(column="id").severity == TestSeverity.ERROR
    assert TestUnique(column="id", severity="warn").severity == TestSeverity.WARN


def _write_project(project_root: Path, severity: TestSeverity) -> Path:
    write_project_marker(project_root)
    (project_root / "db" / "derived").mkdir(parents=True)
    (project_root / "db" / "derived" / "orders.py").write_text(
        "from clair import Table, TestSeverity, TestUnique\n"
        "trouve = Table(\n"
        '    sql="select 1 as id",\n'
        f'    tests=[TestUnique(column="id", severity=TestSeverity("{severity.value}"))],\n'
        ")\n"
    )
    (project_root / "db" / "derived" / "order_count.py").write_text(
        "from clair import Table\n"
        "from db.derived.orders import trouve as orders\n"
        'trouve = Table(sql=f"select count(*) as n from {orders}")\n'
    )
    return project_root


def _promoted(result: RunSuccess | RunFailure) -> bool:
    return any("promote the tested table" in statement.sql for statement in result.statements)


def test_a_warn_test_that_finds_rows_promotes_and_runs_downstream(tmp_path):
    project_root = _write_project(tmp_path / "project", TestSeverity.WARN)

    summary = clair.run(project_root, env=make_environment(), adapter=RecordingAdapter(select_row_count=3))

    upstream = summary.result(UPSTREAM)
    assert isinstance(upstream, RunSuccess)
    assert _promoted(upstream)
    (test_result,) = upstream.test_results
    assert not test_result.passed
    assert not test_result.is_blocking
    assert isinstance(summary.result(DOWNSTREAM), RunSuccess)


def test_an_error_test_that_finds_rows_stops_the_promotion_and_skips_downstream(tmp_path):
    project_root = _write_project(tmp_path / "project", TestSeverity.ERROR)

    summary = clair.run(project_root, env=make_environment(), adapter=RecordingAdapter(select_row_count=3))

    upstream = summary.result(UPSTREAM)
    assert isinstance(upstream, RunFailure)
    assert not _promoted(upstream)
    assert isinstance(summary.result(DOWNSTREAM), RunSkip)


def test_a_warn_test_whose_query_fails_stops_the_promotion(tmp_path):
    project_root = _write_project(tmp_path / "project", TestSeverity.WARN)
    adapter = RecordingAdapter(fail_on=["HAVING COUNT(*) > 1"])

    summary = clair.run(project_root, env=make_environment(), adapter=adapter)

    upstream = summary.result(UPSTREAM)
    assert isinstance(upstream, RunFailure)
    assert not _promoted(upstream)
    assert isinstance(summary.result(DOWNSTREAM), RunSkip)
