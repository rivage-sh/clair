"""Prove against Snowflake that the docs of a Trouve and of its columns become comments.

A mock adapter shows the statements. Only the warehouse shows that the
comment survives the promotion, and that the syntax is correct for a table and
for a view.

The fixture runs one project three times:

1. Each Trouve has docs on the object and on one column.
2. An incremental run with the same docs. The promotion clones a clone of
   the physical table, and the comments must survive the two clones. The
   comments equal the docs, thus clair sends no comment statement.
3. The incremental Trouve has no docs. Its table keeps the comments of the
   earlier runs, so clair must clear them.
"""

from __future__ import annotations

from typing import NamedTuple

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunFailure, RunSuccess, RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "comments"

VIEW_FILE = '''\
from clair import Column, View
trouve = View(
    sql="SELECT 1 AS id",
    docs="""
    The view of the orders.
    It's one row.
    """,
    columns=[Column(name="id", type="NUMBER", docs="The order ID.")],
)
'''


def incremental_file(new_id: int, docs: str, column_docs: str) -> str:
    return (
        "from clair import Append, Column, Incremental, Table\n"
        "trouve = Table(\n"
        f'    sql="SELECT {new_id} AS id",\n'
        "    run_config=Incremental(strategy=Append()),\n"
        f"    docs={docs!r},\n"
        f'    columns=[Column(name="id", type="NUMBER", docs={column_docs!r})],\n'
        ")\n"
    )


class _Comments(NamedTuple):
    object_comment: str
    column_comment: str


def comments_of(adapter: SnowflakeAdapter, address: TrouveAddress) -> _Comments:
    """Read the comment of the object and of its ID column from INFORMATION_SCHEMA."""
    where = (
        f"WHERE TABLE_SCHEMA = '{address.schema_name.upper()}' "
        f"AND TABLE_NAME = '{address.table_name.upper()}'"
    )
    (object_row,) = query_rows(
        adapter, f"SELECT COMMENT FROM {address.database_name}.INFORMATION_SCHEMA.TABLES {where}"
    )
    (column_row,) = query_rows(
        adapter,
        f"SELECT COMMENT FROM {address.database_name}.INFORMATION_SCHEMA.COLUMNS {where} "
        f"AND COLUMN_NAME = 'ID'",
    )
    return _Comments(str(object_row[0] or ""), str(column_row[0] or ""))


class _Step(NamedTuple):
    summary: RunSummary
    table: _Comments
    view: _Comments


class _Runs(NamedTuple):
    documented: _Step
    unchanged: _Step
    undocumented: _Step


def comment_statements_of(summary: RunSummary) -> list[str]:
    """Give each comment statement that the run sent to the warehouse."""
    return [
        statement.sql
        for result in summary.results
        if isinstance(result, RunSuccess | RunFailure)
        for statement in result.statements
        if "-- comment:" in statement.sql
    ]


class TestComments:
    @pytest.fixture(scope="class")
    def runs(
        self,
        environment: Environment,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> _Runs:
        destination = tmp_path_factory.mktemp(DATABASE_NAME)
        schema_name = clair_environment.schema_name
        table_address = physical_address(f"{DATABASE_NAME}.refined.orders", schema_name)
        view_address = physical_address(f"{DATABASE_NAME}.refined.orders_view", schema_name)

        def run(table_file: str, run_mode: RunMode) -> _Step:
            project_path = write_refined_project(
                destination,
                DATABASE_NAME,
                {"orders": table_file, "orders_view": VIEW_FILE},
            )
            summary = clair.run(project_path, env=environment, run_mode=run_mode)
            return _Step(
                summary, comments_of(adapter, table_address), comments_of(adapter, view_address)
            )

        return _Runs(
            documented=run(
                incremental_file(1, "The orders.", "The order ID."), RunMode.FULL_REFRESH
            ),
            unchanged=run(
                incremental_file(2, "The orders.", "The order ID."), RunMode.INCREMENTAL
            ),
            undocumented=run(incremental_file(3, "", ""), RunMode.INCREMENTAL),
        )

    @pytest.mark.parametrize("step_name", ["documented", "unchanged", "undocumented"])
    def test_each_run_succeeds(self, runs: _Runs, step_name: str) -> None:
        step: _Step = getattr(runs, step_name)
        assert step.summary.failed == [], [result.error for result in step.summary.failed]

    def test_a_promoted_table_holds_the_comments(self, runs: _Runs) -> None:
        assert runs.documented.table == _Comments("The orders.", "The order ID.")

    def test_a_promoted_view_holds_the_comments(self, runs: _Runs) -> None:
        assert runs.documented.view == _Comments(
            "The view of the orders.\nIt's one row.", "The order ID."
        )

    def test_an_unchanged_incremental_run_keeps_the_comments_and_writes_none(
        self, runs: _Runs
    ) -> None:
        assert runs.unchanged.table == _Comments("The orders.", "The order ID.")
        table_statements = [
            sql for sql in comment_statements_of(runs.unchanged.summary) if "orders_view" not in sql.lower()
        ]
        assert table_statements == []

    def test_an_incremental_run_clears_the_comments_that_the_docs_no_longer_hold(
        self, runs: _Runs
    ) -> None:
        assert runs.undocumented.table == _Comments("", "")
