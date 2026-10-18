"""A transient fault runs the node again, but only when a second run gives the same data.

A staged run and a full refresh make their object again on each attempt. An
incremental run with no staging changes the physical table, and a statement
that failed on the network can still have committed. A fault in the promotion
can come after the physical table changed. Neither of the two runs again.
"""

from __future__ import annotations

import pytest
from snowflake.connector.errors import OperationalError, ProgrammingError

from clair.adapters.snowflake import is_retryable_error
from clair.core.runner import RunStatus
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.trouve import Table
from tests.helpers import RecordingAdapter, run_one_trouve, snowflake_adapter_that_answers

BUILD_TEXT = "CREATE OR REPLACE TABLE"
INSERT_TEXT = "INSERT INTO"
PROMOTE_TEXT = "promote the tested table"


def full_refresh_table() -> Table:
    return Table(sql="SELECT 1 AS id")


def incremental_table() -> Table:
    return Table(sql="SELECT 1 AS id", run_config=Incremental(strategy=Append()))


class TestASafeRetry:
    @pytest.mark.parametrize(
        ("trouve", "run_mode", "use_staging", "failing_text"),
        [
            pytest.param(full_refresh_table(), RunMode.FULL_REFRESH, True, BUILD_TEXT, id="staged_full_refresh"),
            pytest.param(full_refresh_table(), RunMode.FULL_REFRESH, False, BUILD_TEXT, id="full_refresh_with_no_staging"),
            pytest.param(incremental_table(), RunMode.INCREMENTAL, True, INSERT_TEXT, id="staged_incremental"),
        ],
    )
    def test_one_transient_fault_runs_the_node_again_and_it_succeeds(
        self, trouve, run_mode, use_staging, failing_text
    ):
        adapter = RecordingAdapter(transient_failures={failing_text: 1})
        result = run_one_trouve(trouve, adapter, run_mode=run_mode, use_staging=use_staging)

        assert result.status == RunStatus.SUCCESS, result.error
        assert result.attempt_count == 2

    def test_the_retry_count_limits_the_attempts(self):
        adapter = RecordingAdapter(transient_failures={BUILD_TEXT: 5})
        result = run_one_trouve(full_refresh_table(), adapter, retries=2)

        assert result.status == RunStatus.FAILURE
        assert result.attempt_count == 3

    def test_zero_retries_runs_the_node_one_time(self):
        adapter = RecordingAdapter(transient_failures={BUILD_TEXT: 1})
        result = run_one_trouve(full_refresh_table(), adapter, retries=0)

        assert result.status == RunStatus.FAILURE
        assert result.attempt_count == 1


class TestNoRetry:
    def test_an_incremental_insert_with_no_staging_does_not_run_again(self):
        """The INSERT can have committed before the network fault. A second one gives each row two times."""
        adapter = RecordingAdapter(transient_failures={INSERT_TEXT: 1})
        result = run_one_trouve(
            incremental_table(), adapter, run_mode=RunMode.INCREMENTAL, use_staging=False
        )

        assert result.status == RunStatus.FAILURE
        assert result.attempt_count == 1

    def test_a_fault_in_the_promotion_does_not_run_again(self):
        """The physical table can already hold the new rows."""
        adapter = RecordingAdapter(transient_failures={PROMOTE_TEXT: 1})
        result = run_one_trouve(incremental_table(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.FAILURE
        assert result.attempt_count == 1

    def test_a_fault_of_the_sql_does_not_run_again(self):
        adapter = RecordingAdapter(fail_on=[BUILD_TEXT])
        result = run_one_trouve(full_refresh_table(), adapter)

        assert result.status == RunStatus.FAILURE
        assert result.attempt_count == 1


class TestTheSnowflakeClassification:
    @pytest.mark.parametrize(
        ("error", "retryable"),
        [
            (OperationalError(msg="connection reset", errno=250001), True),
            (ProgrammingError(msg="invalid identifier", errno=904), False),
            (OperationalError(msg="statement reached its timeout", errno=630), False),
            (OperationalError(msg="statement cancelled", errno=604), False),
            (ValueError("not a driver error"), False),
        ],
    )
    def test_only_a_fault_of_the_network_or_the_service_is_retryable(self, error, retryable):
        assert is_retryable_error(error) is retryable

    def test_execute_marks_the_failed_statement(self):
        adapter, _ = snowflake_adapter_that_answers(
            [], error=OperationalError(msg="connection reset", errno=250001)
        )
        statement = adapter.execute("SELECT 1")
        assert statement.retryable
