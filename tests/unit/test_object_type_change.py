"""The tests of a Trouve that changes between a TABLE and a VIEW.

Snowflake replaces an object with an object of the same type only. Clair does
not drop the old object, because a drop removes each grant on it. A staged run
builds and tests the candidate, then stops before the promotion and keeps the
candidate. A run with no staging stops before any write. The error names the
drop that a person must run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar, cast

import pytest
from structlog.testing import capture_logs

from clair.adapters.base import StatementStatus, WarehouseObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.runner import RunResult, RunStatus, run_project, wanted_object_type
from clair.core.staging import make_staging_address
from clair.environments.routing import TrouveAddress
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.trouve import (
    CompiledAttributes,
    ExecutionType,
    SqlTrouve,
    Table,
    TrouveAbc,
    TrouveType,
    View,
)
from tests.helpers import RecordingAdapter

RUN_ID = "0195aabbccddeeff0011223344556677"
ADDRESS = "analytics.revenue.orders"

# The staging address starts with the physical address, thus a test that looks
# for a physical statement by its address alone also finds the staging one.
# Each statement carries a comment that names its step, so the tests match that.
SEED_CLONE = "clone the target"
PROMOTE_TABLE = "promote the tested table"
PROMOTE_VIEW = "promote the tested view"

AnyTrouve = TypeVar("AnyTrouve", bound=TrouveAbc)


def _compile(trouve: AnyTrouve, address: str = ADDRESS) -> AnyTrouve:
    """Give the Trouve the compiled attributes that the runner needs."""
    trouve.compiled = CompiledAttributes(
        physical_address=TrouveAddress.parse(address),
        logical_address=TrouveAddress.parse(address),
        resolved_sql=getattr(trouve, "sql", ""),
        file_path=Path(f"/fake/{address.replace('.', '/')}.py"),
        module_name=address,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return trouve


def _staging_address(address: str = ADDRESS) -> str:
    return str(make_staging_address(TrouveAddress.parse(address), RUN_ID))


def _run(
    trouve: SqlTrouve,
    adapter: RecordingAdapter,
    *,
    use_staging: bool = True,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    tests_passed: bool = True,
    tested_addresses: list[str] | None = None,
) -> RunResult:
    """Run one Trouve, and give the single RunResult.

    *tested_addresses* receives the address of each object that the data
    quality tests examined.
    """

    def run_the_tests(name: str, address: str, _adapter: object) -> bool:
        if tested_addresses is not None:
            tested_addresses.append(address)
        return tests_passed

    dag = build_dag([_compile(trouve)])
    selected = get_executable_nodes(dag)
    results = list(
        run_project(
            dag,
            selected,
            adapter,
            run_mode=run_mode,
            run_id=RUN_ID,
            after_node_success=run_the_tests if use_staging else None,
            use_staging=use_staging,
        )
    )
    assert len(results) == 1
    return results[0]


def _executed_sql(result: RunResult) -> list[str]:
    """Give the SQL of each statement that clair sent to the warehouse."""
    return [
        statement.sql
        for statement in result.statements
        if statement.status != StatementStatus.NOT_RUN
    ]


def _drops_of_the_physical_object(result: RunResult) -> list[str]:
    """Give each statement that drops the physical address, and not the staging one."""
    return [
        sql
        for sql in _executed_sql(result)
        if "DROP" in sql and _staging_address() not in sql
    ]


class TestWantedObjectType:
    def test_a_table_trouve_wants_a_table(self):
        assert wanted_object_type(TrouveType.TABLE) == WarehouseObjectType.TABLE

    def test_a_view_trouve_wants_a_view(self):
        assert wanted_object_type(TrouveType.VIEW) == WarehouseObjectType.VIEW


class TestStagedRun:
    def test_a_table_that_becomes_a_view_stops_before_the_promotion(self):
        # RecordingAdapter answers TABLE for each address, thus the physical
        # address holds a table and the Trouve asks for a view.
        adapter = RecordingAdapter()
        result = _run(View(sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.FAILURE
        assert f"{ADDRESS} holds a TABLE, and the Trouve is now a VIEW" in result.error
        assert f"Run DROP TABLE {ADDRESS}" in result.error
        assert not [sql for sql in _executed_sql(result) if PROMOTE_VIEW in sql]
        assert _drops_of_the_physical_object(result) == []

    def test_a_view_that_becomes_a_table_stops_before_the_promotion(self):
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        result = _run(Table(sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.FAILURE
        assert f"{ADDRESS} holds a VIEW, and the Trouve is now a TABLE" in result.error
        assert f"Run DROP VIEW {ADDRESS}" in result.error
        assert not [sql for sql in _executed_sql(result) if PROMOTE_TABLE in sql]
        assert _drops_of_the_physical_object(result) == []

    def test_the_candidate_is_built_and_tested_and_kept(self):
        # A person who reads the error can query a tested candidate, so the
        # stop comes after the build and after the tests.
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        tested_addresses: list[str] = []
        result = _run(
            Table(sql="SELECT 1 AS id"),
            adapter,
            tested_addresses=tested_addresses,
        )

        assert f"CREATE OR REPLACE TABLE {_staging_address()}" in _executed_sql(result)[0]
        assert tested_addresses == [_staging_address()]
        assert f"The tested candidate stays at {_staging_address()}" in result.error
        # Clair drops the staging object only after a promotion.
        assert not [sql for sql in _executed_sql(result) if "DROP" in sql]

    def test_a_failed_test_reports_the_test_and_not_the_type(self):
        # The tests come first. A candidate that fails them is the first fault
        # to fix, and the type of the address does not matter yet.
        adapter = RecordingAdapter()
        result = _run(
            View(sql="SELECT 1 AS id"),
            adapter,
            tests_passed=False,
        )

        assert result.status == RunStatus.FAILURE
        assert "the tests failed" in result.error
        assert "holds a TABLE" not in result.error

    def test_a_type_that_does_not_change_promotes(self):
        adapter = RecordingAdapter()
        result = _run(Table(sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.SUCCESS
        assert [sql for sql in _executed_sql(result) if PROMOTE_TABLE in sql]

    def test_an_address_that_holds_nothing_promotes(self):
        # After the person drops the old object, the next run takes this path.
        adapter = RecordingAdapter(existing_tables=[])
        result = _run(View(sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.SUCCESS
        assert [sql for sql in _executed_sql(result) if PROMOTE_VIEW in sql]

    def test_the_failure_is_in_the_log(self):
        adapter = RecordingAdapter()
        with capture_logs() as log_entries:
            _run(View(sql="SELECT 1 AS id"), adapter)

        failures = [e for e in log_entries if e["event"] == "run.node.failure"]
        assert len(failures) == 1
        assert f"Run DROP TABLE {ADDRESS}" in failures[0]["error"]


class TestRunWithNoStaging:
    def test_a_type_change_stops_before_any_write(self):
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        result = _run(
            Table(sql="SELECT 1 AS id"),
            adapter,
            use_staging=False,
        )

        assert result.status == RunStatus.FAILURE
        assert f"Run DROP VIEW {ADDRESS}" in result.error
        # No staging step, thus no candidate to keep, and no word about one.
        assert "candidate" not in result.error
        assert result.statements == []

    def test_a_type_that_does_not_change_builds(self):
        adapter = RecordingAdapter()
        result = _run(
            Table(sql="SELECT 1 AS id"),
            adapter,
            use_staging=False,
        )

        assert result.status == RunStatus.SUCCESS
        assert f"CREATE OR REPLACE TABLE {ADDRESS}" in _executed_sql(result)[0]


class TestIncrementalFallback:
    def _incremental_trouve(self) -> Table:
        return Table(
            sql="SELECT 1 AS id",
            run_config=Incremental(strategy=Append()),
        )

    def test_a_view_at_the_address_builds_a_full_candidate_and_stops(self):
        # An incremental run needs a base table, and a view is not one. Clair
        # builds the full table as the candidate, tests it, and then stops,
        # because the address holds a view.
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        with capture_logs() as log_entries:
            result = _run(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert result.status == RunStatus.FAILURE
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert len(fallbacks) == 1
        assert fallbacks[0]["reason"] == "object_type_changed"
        # A clone of a view into a table is an error in Snowflake, thus the
        # plan must hold no seed clone.
        assert not [sql for sql in _executed_sql(result) if SEED_CLONE in sql]
        assert f"Run DROP VIEW {ADDRESS}" in result.error

    def test_an_address_that_holds_nothing_changes_the_run_to_a_full_refresh(self):
        adapter = RecordingAdapter(existing_tables=[])
        with capture_logs() as log_entries:
            result = _run(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert result.status == RunStatus.SUCCESS
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert fallbacks[0]["reason"] == "object_not_found"

    def test_a_table_at_the_address_keeps_the_incremental_mode(self):
        adapter = RecordingAdapter()
        result = _run(self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.SUCCESS
        assert result.effective_run_mode == RunMode.INCREMENTAL
        assert [sql for sql in _executed_sql(result) if SEED_CLONE in sql]


class _FakeCursor:
    """A cursor that gives one row, or raises. It records the SQL that it got."""

    def __init__(self, row: tuple | None, error: Exception | None = None) -> None:
        self.row = row
        self.error = error
        self.sql = ""
        self.closed = False

    def execute(self, sql: str) -> None:
        self.sql = sql
        if self.error is not None:
            raise self.error

    def fetchone(self) -> tuple | None:
        return self.row

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _snowflake_adapter_that_answers(
    row: tuple | None, error: Exception | None = None
) -> tuple[SnowflakeAdapter, _FakeCursor]:
    """Give a SnowflakeAdapter whose connection answers one row."""
    adapter = SnowflakeAdapter()
    cursor = _FakeCursor(row, error)
    # The adapter needs a connection object only to make a cursor, thus the
    # fake gives one. cast keeps the type checker quiet about the narrow use.
    adapter._conn = cast(Any, _FakeConnection(cursor))
    return adapter, cursor


class TestSnowflakeObjectType:
    """The map from the TABLE_TYPE column of Snowflake onto WarehouseObjectType."""

    def test_a_base_table_is_a_table(self):
        adapter, _ = _snowflake_adapter_that_answers(("BASE TABLE",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.TABLE

    def test_a_view_is_a_view(self):
        adapter, _ = _snowflake_adapter_that_answers(("VIEW",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.VIEW

    def test_a_materialized_view_is_a_view(self):
        # Clair makes no materialized view, but a user can point a Trouve at an
        # address that holds one. A view is the closer answer of the two.
        adapter, _ = _snowflake_adapter_that_answers(("MATERIALIZED VIEW",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.VIEW

    def test_no_row_means_the_address_holds_nothing(self):
        adapter, _ = _snowflake_adapter_that_answers(None)
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) is None

    def test_a_query_that_fails_means_the_address_holds_nothing(self):
        # A database that does not exist makes the query fail. Clair then makes
        # the object, and Snowflake gives the clear error.
        adapter, cursor = _snowflake_adapter_that_answers(
            None, error=RuntimeError("Database 'ANALYTICS' does not exist")
        )
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) is None
        assert cursor.closed

    def test_the_query_reads_the_information_schema_of_the_database(self):
        adapter, cursor = _snowflake_adapter_that_answers(("BASE TABLE",))
        adapter.object_type(TrouveAddress.parse(ADDRESS))
        assert "analytics.INFORMATION_SCHEMA.TABLES" in cursor.sql
        assert "TABLE_SCHEMA = 'REVENUE'" in cursor.sql
        assert "TABLE_NAME = 'ORDERS'" in cursor.sql

    def test_an_adapter_with_no_connection_raises(self):
        adapter = SnowflakeAdapter()
        with pytest.raises(RuntimeError, match="Not connected"):
            adapter.object_type(TrouveAddress.parse(ADDRESS))
