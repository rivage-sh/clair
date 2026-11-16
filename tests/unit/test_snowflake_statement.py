"""The Statement that SnowflakeAdapter.execute() gives: the row counts, the query ID and the URL.

A probe in Snowflake showed the answers that the connector gives. A CREATE
TABLE AS SELECT has ``rowcount`` 1, its status row, whatever it wrote. A
statement that wrote rows gives DML statistics. A statement that wrote no rows,
a query and a DDL statement give none. The rows of the cursor are the rows of
the result, thus ``rowcount`` here is their number.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from snowflake.connector.cursor import QueryResultStats

from clair.adapters.snowflake import SnowflakeAdapter
from tests.helpers import FakeConnection, FakeCursor, make_environment

NO_STATS = QueryResultStats(None, None, None, None)


def _adapter(cursor: FakeCursor, **connection_fields: str) -> SnowflakeAdapter:
    adapter = SnowflakeAdapter()
    adapter._conn = cast(Any, FakeConnection(cursor))
    adapter._environment = make_environment("dev", None, **connection_fields)
    return adapter


STATUS_ROW = [("Table PROBE successfully created.",)]


@pytest.mark.parametrize(
    ("rows", "stats", "expected_row_count", "expected_written_row_count"),
    [
        pytest.param(STATUS_ROW, QueryResultStats(7, 0, 0, 0), 1, 7, id="create_table_as_select"),
        # Found in CI: an empty table gives no statistics, and still one status row.
        pytest.param(STATUS_ROW, NO_STATS, 1, 0, id="create_table_as_select_of_no_row"),
        pytest.param([(3,)], QueryResultStats(3, 0, 0, 0), 1, 3, id="insert"),
        pytest.param([(1, 2)], QueryResultStats(1, 0, 2, 0), 1, 3, id="merge"),
        pytest.param([(2,)], QueryResultStats(0, 2, 0, 0), 1, 2, id="delete"),
        pytest.param([("View PROBE successfully created.",)], NO_STATS, 1, 0, id="create_view"),
        # A test query: each row is a failure, thus row_count must count the rows.
        pytest.param([(1,), (2,), (3,)], NO_STATS, 3, 0, id="query"),
        pytest.param([("x",)], NO_STATS, 1, 0, id="query_of_one_status_column"),
        pytest.param([], None, 0, 0, id="query_with_no_row"),
    ],
)
def test_the_statement_keeps_the_rows_of_the_result_and_the_rows_written_apart(
    rows: list[tuple],
    stats: QueryResultStats | None,
    expected_row_count: int,
    expected_written_row_count: int,
) -> None:
    adapter = _adapter(FakeCursor(rows, stats=stats))

    statement = adapter.execute("SELECT 1")

    assert (statement.row_count, statement.written_row_count) == (
        expected_row_count,
        expected_written_row_count,
    )


@pytest.mark.parametrize(
    ("sfqid", "connection_fields", "expected_url"),
    [
        pytest.param(
            "01b2",
            {"region": "us-east-1", "account_locator": "ab12345"},
            "https://app.snowflake.com/us-east-1/ab12345/#/compute/history/queries/01b2/detail",
            id="complete",
        ),
        pytest.param("01b2", {"account_locator": "ab12345"}, None, id="no_region"),
        pytest.param("01b2", {"region": "us-east-1"}, None, id="no_account_locator"),
        pytest.param(None, {"region": "us-east-1", "account_locator": "ab12345"}, None, id="no_query_id"),
    ],
)
def test_a_statement_gets_a_url_only_when_each_part_of_it_exists(
    sfqid: str | None, connection_fields: dict[str, str], expected_url: str | None
) -> None:
    adapter = _adapter(FakeCursor([], sfqid=sfqid), **connection_fields)

    statement = adapter.execute("SELECT 1")

    assert (statement.query_id, statement.query_url) == (sfqid, expected_url)


def test_a_statement_that_fails_before_snowflake_has_no_query_id() -> None:
    cursor = FakeCursor([], error=ConnectionError("the network is down"), sfqid=None)
    adapter = _adapter(cursor, region="us-east-1", account_locator="ab12345")

    statement = adapter.execute("SELECT 1")

    assert not statement.success
    assert (statement.query_id, statement.query_url) == (None, None)
