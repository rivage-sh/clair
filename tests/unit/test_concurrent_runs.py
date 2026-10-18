"""Two incremental runs of one table must not remove the rows of each other.

Each run clones the table, adds its rows to the clone, and promotes the clone.
The second promotion replaces the table, thus the rows of the first run
disappear. Clair reads the change token of the table before the clone and
before the promotion, and does not promote when the token changed.
"""

from __future__ import annotations

import pytest

from clair.core.runner import RunStatus
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.trouve import Table
from tests.helpers import (
    RecordingAdapter,
    executed_sql,
    run_one_trouve,
    snowflake_adapter_that_answers,
)

PROMOTE_TEXT = "promote the tested table"


def incremental_table() -> Table:
    return Table(sql="SELECT 1 AS id", run_config=Incremental(strategy=Append()))


class TestTheGuard:
    def test_a_change_after_the_clone_stops_the_promotion_and_keeps_the_candidate(self):
        adapter = RecordingAdapter(concurrent_change=True)
        result = run_one_trouve(incremental_table(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.FAILURE
        assert "another run changed" in result.error
        assert "The tested candidate stays at" in result.error
        assert not [sql for sql in executed_sql(result) if PROMOTE_TEXT in sql]

    def test_no_change_promotes(self):
        adapter = RecordingAdapter()
        result = run_one_trouve(incremental_table(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.SUCCESS
        assert adapter.change_token_reads == 2

    @pytest.mark.parametrize(
        ("trouve", "run_mode", "use_staging"),
        [
            pytest.param(Table(sql="SELECT 1 AS id"), RunMode.FULL_REFRESH, True, id="a_full_refresh_makes_the_whole_table"),
            pytest.param(incremental_table(), RunMode.INCREMENTAL, False, id="no_staging_has_no_promotion"),
        ],
    )
    def test_a_run_with_no_clone_to_promote_reads_no_token(self, trouve, run_mode, use_staging):
        adapter = RecordingAdapter(concurrent_change=True)
        result = run_one_trouve(trouve, adapter, run_mode=run_mode, use_staging=use_staging)

        assert result.status == RunStatus.SUCCESS
        assert adapter.change_token_reads == 0


class TestSnowflakeChangeToken:
    def test_it_reads_the_last_change_commit_time(self):
        adapter, cursor = snowflake_adapter_that_answers([(1760000000000000000,)])
        token = adapter.change_token(TrouveAddress.parse("db.s.orders"))

        assert token == "1760000000000000000"
        assert cursor.sql == "SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('db.s.orders')"

    def test_a_query_that_fails_gives_none(self):
        adapter, _ = snowflake_adapter_that_answers([], error=RuntimeError("no such table"))
        assert adapter.change_token(TrouveAddress.parse("db.s.orders")) is None
