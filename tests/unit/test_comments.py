"""The docs of a Trouve and of its columns become comments in the warehouse.

A promotion of a view is a CREATE OR REPLACE VIEW, and it removes each comment
of the staging view. Thus the comments go onto the physical object after the
promotion. An incremental table keeps its comments between runs, thus it gets
an empty comment for each text that is empty.
"""

from __future__ import annotations

import pandas as pd
import pytest

from clair.core.comments import build_comment_statements, quote_comment
from clair.core.compiler import build_statements
from clair.core.runner import RunStatus
from clair.core.staging import make_staging_address
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column
from clair.trouves.pandas_table import PandasTable
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.trouve import Iceberg, Table, View
from tests.helpers import (
    TEST_ADDRESS,
    TEST_RUN_ID,
    RecordingAdapter,
    compile_trouve,
    executed_sql,
    run_one_trouve,
)

PHYSICAL_ADDRESS = TrouveAddress.parse(TEST_ADDRESS)
STAGING_ADDRESS = make_staging_address(PHYSICAL_ADDRESS, TEST_RUN_ID)
PROMOTE_TABLE = "promote the tested table"
PROMOTE_VIEW = "promote the tested view"
DROP_STAGING = "drop the staging object"

TABLE_COMMENT = f"ALTER TABLE {PHYSICAL_ADDRESS} SET COMMENT = 'The orders.'"
VIEW_COMMENT = f"ALTER VIEW {PHYSICAL_ADDRESS} SET COMMENT = 'The orders.'"
COLUMN_COMMENT = f"COMMENT ON COLUMN {PHYSICAL_ADDRESS}.id IS 'The order ID.'"


def documented_columns() -> list[Column]:
    return [
        Column(name="id", type="NUMBER", docs="The order ID."),
        Column(name="amount", type="NUMBER"),
    ]


def index_of(sql_texts: list[str], expected: str) -> int:
    """Give the position of the first statement that holds the text, or -1."""
    return next((position for position, sql in enumerate(sql_texts) if expected in sql), -1)


def comment_statements(sql_texts: list[str]) -> list[str]:
    return [sql for sql in sql_texts if "COMMENT" in sql]


class TestTheText:
    def test_a_quote_a_backslash_and_a_line_break_are_escaped(self):
        assert quote_comment("it's a\\b\nc") == "'it\\'s a\\\\b\\nc'"

    def test_a_triple_quoted_text_loses_its_indent(self):
        trouve = Table(sql="SELECT 1 AS id", docs="\n    The orders.\n    One row each.\n    ")

        (statement,) = build_comment_statements(PHYSICAL_ADDRESS, trouve)

        assert statement.endswith("SET COMMENT = 'The orders.\\nOne row each.'")

    def test_each_statement_is_one_line_after_its_header(self):
        trouve = Table(sql="SELECT 1 AS id", docs="a\nb", columns=documented_columns())

        for statement in build_comment_statements(PHYSICAL_ADDRESS, trouve):
            assert len(statement.splitlines()) == 2


class TestTheStatements:
    def test_a_trouve_with_no_docs_gets_no_statement(self):
        trouve = Table(sql="SELECT 1 AS id", columns=[Column(name="id", type="NUMBER")])

        assert build_comment_statements(PHYSICAL_ADDRESS, trouve) == []

    def test_only_a_column_with_docs_gets_a_comment(self):
        trouve = Table(sql="SELECT 1 AS id", docs="The orders.", columns=documented_columns())

        statements = build_comment_statements(PHYSICAL_ADDRESS, trouve)

        assert [statement.splitlines()[-1] for statement in statements] == [
            TABLE_COMMENT,
            COLUMN_COMMENT,
        ]

    def test_a_view_gets_an_alter_view(self):
        trouve = View(sql="SELECT 1 AS id", docs="The orders.")

        assert build_comment_statements(PHYSICAL_ADDRESS, trouve)[0].endswith(VIEW_COMMENT)

    def test_an_iceberg_table_gets_an_alter_iceberg_table(self):
        trouve = Table(sql="SELECT 1 AS id", docs="The orders.", iceberg=Iceberg())

        (statement,) = build_comment_statements(PHYSICAL_ADDRESS, trouve)

        assert f"ALTER ICEBERG TABLE {PHYSICAL_ADDRESS} SET COMMENT" in statement

    def test_an_incremental_table_clears_each_empty_text(self):
        trouve = Table(
            sql="SELECT 1 AS id",
            run_config=Incremental(strategy=Append()),
            columns=documented_columns(),
        )

        statements = build_comment_statements(PHYSICAL_ADDRESS, trouve)

        assert [statement.splitlines()[-1] for statement in statements] == [
            f"ALTER TABLE {PHYSICAL_ADDRESS} SET COMMENT = ''",
            COLUMN_COMMENT,
            f"COMMENT ON COLUMN {PHYSICAL_ADDRESS}.amount IS ''",
        ]


class TestThePlan:
    @pytest.mark.parametrize(
        ("trouve", "promotion", "comment"),
        [
            (Table(sql="SELECT 1 AS id", docs="The orders."), PROMOTE_TABLE, TABLE_COMMENT),
            (View(sql="SELECT 1 AS id", docs="The orders."), PROMOTE_VIEW, VIEW_COMMENT),
        ],
        ids=["table", "view"],
    )
    def test_a_staged_plan_writes_the_comment_after_the_promotion(
        self, trouve: Table | View, promotion: str, comment: str
    ):
        statements = build_statements(compile_trouve(trouve), RunMode.FULL_REFRESH, TEST_RUN_ID)

        promotion_position = index_of(statements, promotion)
        comment_position = index_of(statements, comment)
        assert -1 < promotion_position < comment_position < index_of(statements, DROP_STAGING)
        assert index_of(statements, f"{STAGING_ADDRESS} SET COMMENT") == -1

    def test_a_plan_with_no_staging_writes_the_comment_after_the_build(self):
        trouve = Table(sql="SELECT 1 AS id", docs="The orders.")

        statements = build_statements(
            compile_trouve(trouve), RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=False
        )

        build_position = index_of(statements, f"CREATE OR REPLACE TABLE {PHYSICAL_ADDRESS} AS")
        assert -1 < build_position < index_of(statements, TABLE_COMMENT)


class TestTheRun:
    @pytest.mark.parametrize(
        "trouve",
        [
            Table(sql="SELECT 1 AS id", docs="The orders."),
            PandasTable(transform=lambda: pd.DataFrame({"id": [1]}), docs="The orders."),
        ],
        ids=["Table", "PandasTable"],
    )
    def test_a_staged_run_writes_the_comment_after_the_promotion(self, trouve: Table | PandasTable):
        result = run_one_trouve(trouve, RecordingAdapter())

        assert result.status == RunStatus.SUCCESS
        sql_texts = executed_sql(result)
        assert -1 < index_of(sql_texts, PROMOTE_TABLE) < index_of(sql_texts, TABLE_COMMENT)
        assert len(comment_statements(sql_texts)) == 1

    @pytest.mark.parametrize(
        "trouve",
        [
            Table(sql="SELECT 1 AS id", docs="The orders."),
            PandasTable(transform=lambda: pd.DataFrame({"id": [1]}), docs="The orders."),
        ],
        ids=["Table", "PandasTable"],
    )
    def test_a_run_with_no_staging_writes_the_comment_after_the_build(
        self, trouve: Table | PandasTable
    ):
        result = run_one_trouve(trouve, RecordingAdapter(), use_staging=False)

        assert result.status == RunStatus.SUCCESS
        sql_texts = executed_sql(result)
        assert index_of(sql_texts, TABLE_COMMENT) == len(sql_texts) - 1
        assert len(comment_statements(sql_texts)) == 1

    def test_a_comment_that_snowflake_rejects_fails_the_node_after_the_promotion(self):
        adapter = RecordingAdapter(fail_on=["SET COMMENT"])

        result = run_one_trouve(Table(sql="SELECT 1 AS id", docs="The orders."), adapter)

        assert result.status == RunStatus.FAILURE
        assert index_of(executed_sql(result), PROMOTE_TABLE) > -1
        assert f"{PHYSICAL_ADDRESS} holds the tested data" in result.error
        assert TABLE_COMMENT in result.error
