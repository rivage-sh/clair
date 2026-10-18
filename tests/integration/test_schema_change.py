"""Prove what an incremental run does when its SQL gives a new column, in Snowflake.

The fixture builds a table, then adds a column to the SQL three times:

1. With FAIL. The run stops, and the table keeps its columns and its rows.
2. With ADD_NEW_COLUMNS and a test that fails. The ALTER lands on the staging
   clone only, thus the table keeps its columns.
3. With ADD_NEW_COLUMNS. The table gets the column, and the old row gets NULL.
"""

from __future__ import annotations

from typing import NamedTuple

import pytest

import clair
from clair.adapters.base import WarehouseColumn
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "schema_change"
LOGICAL_NAME = f"{DATABASE_NAME}.refined.grown"
FIRST_SQL = "SELECT 1 AS id, 'a' AS label"
GROWN_SQL = "SELECT 2 AS id, 'b' AS label, 7 AS discount"


def trouve_file(sql: str, on_schema_change: str, tests: str = "[]") -> str:
    return (
        "from clair import Append, Incremental, OnSchemaChange, Table, TestRowCount\n"
        "trouve = Table(\n"
        f'    sql="{sql}",\n'
        f"    run_config=Incremental(strategy=Append(), on_schema_change=OnSchemaChange.{on_schema_change}),\n"
        f"    tests={tests},\n"
        ")\n"
    )


def column_names(columns: list[WarehouseColumn] | None) -> list[str]:
    assert columns is not None
    return [column.name for column in columns]


class _Runs(NamedTuple):
    fail: RunSummary
    columns_after_fail: list[str]
    failed_test: RunSummary
    columns_after_failed_test: list[str]
    added: RunSummary
    rows_after_added: list[tuple[object, ...]]


class TestANewColumn:
    @pytest.fixture(scope="class")
    def runs(
        self,
        environment: Environment,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
        workspace_prefix: str,
    ) -> _Runs:
        destination = tmp_path_factory.mktemp(DATABASE_NAME)
        address = physical_address(LOGICAL_NAME, clair_environment.schema_name)

        def run(sql: str, on_schema_change: str, tests: str = "[]") -> RunSummary:
            project_path = write_refined_project(
                destination, DATABASE_NAME, {"grown": trouve_file(sql, on_schema_change, tests)}
            )
            return clair.run(project_path, env=environment, run_mode=RunMode.INCREMENTAL)

        first = run(FIRST_SQL, "FAIL")
        assert first.failed_count == 0, [result.error for result in first.failed]

        fail = run(GROWN_SQL, "FAIL")
        columns_after_fail = column_names(adapter.describe_columns(address))

        # A minimum of 100 rows fails, thus clair keeps the candidate.
        failed_test = run(GROWN_SQL, "ADD_NEW_COLUMNS", "[TestRowCount(min_rows=100)]")
        columns_after_failed_test = column_names(adapter.describe_columns(address))

        added = run(GROWN_SQL, "ADD_NEW_COLUMNS")
        rows_after_added = query_rows(adapter, f"SELECT id, label, discount FROM {address} ORDER BY id")

        return _Runs(fail, columns_after_fail, failed_test, columns_after_failed_test, added, rows_after_added)

    def test_fail_stops_the_run_and_names_the_column(self, runs: _Runs) -> None:
        result = runs.fail.result(LOGICAL_NAME)
        assert result.status == RunStatus.FAILURE
        assert "DISCOUNT" in result.error

    def test_fail_leaves_the_columns_of_the_table(self, runs: _Runs) -> None:
        assert runs.columns_after_fail == ["ID", "LABEL"]

    def test_a_failed_test_leaves_the_columns_of_the_table(self, runs: _Runs) -> None:
        """The ALTER lands on the staging clone, and clair does not promote it."""
        assert runs.failed_test.result(LOGICAL_NAME).status == RunStatus.FAILURE
        assert runs.columns_after_failed_test == ["ID", "LABEL"]

    def test_add_new_columns_adds_the_column_and_the_old_row_gets_null(self, runs: _Runs) -> None:
        result = runs.added.result(LOGICAL_NAME)
        assert result.status == RunStatus.SUCCESS, result.error
        assert runs.rows_after_added == [(1, "a", None), (2, "b", 7)]
