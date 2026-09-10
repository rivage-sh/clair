"""The tests of a Trouve that changes between a TABLE and a VIEW.

Snowflake replaces an object with an object of the same type only. A Trouve that
changes its type therefore needs a drop before the write. Only the warehouse
knows the type that an address holds now, thus the runner asks it, and the
compiler puts a comment in the plan.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar, cast

import pytest
from structlog.testing import capture_logs

from clair.adapters.base import ObjectType, StatementStatus
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.runner import RunResult, RunStatus, run_project, wanted_object_type
from clair.core.staging import build_drop_physical_statement, make_staging_address
from clair.environments.routing import TrouveAddress
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode
from clair.trouves.trouve import (
    CompiledAttributes,
    ExecutionType,
    Trouve,
    TrouveAbc,
    TrouveType,
)
from tests.helpers import RecordingAdapter

RUN_ID = "0195aabbccddeeff0011223344556677"
ADDRESS = "analytics.revenue.orders"

# The staging address starts with the physical address, thus a test that looks
# for a physical statement by its address alone also finds the staging one.
# Each statement carries a comment that names its step, so the tests match that.
TYPE_CHANGE_DROP = "the Trouve changed its type"
SEED_CLONE = "clone the target"
PROMOTE_TABLE = "promote the tested table"
PROMOTE_VIEW = "promote the tested view"
DROP_STAGING = "drop the staging object"

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
    trouve: Trouve,
    adapter: RecordingAdapter,
    *,
    use_staging: bool = True,
    run_mode: RunMode = RunMode.FULL_REFRESH,
    tests_passed: bool = True,
) -> RunResult:
    """Run one Trouve, and give the single RunResult."""
    dag = build_dag([_compile(trouve)])
    selected = get_executable_nodes(dag)
    results = list(
        run_project(
            dag,
            selected,
            adapter,
            run_mode=run_mode,
            run_id=RUN_ID,
            after_node_success=(lambda *_: tests_passed) if use_staging else None,
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


class TestWantedObjectType:
    def test_a_table_trouve_wants_a_table(self):
        assert wanted_object_type(TrouveType.TABLE) == ObjectType.TABLE

    def test_a_view_trouve_wants_a_view(self):
        assert wanted_object_type(TrouveType.VIEW) == ObjectType.VIEW


class TestDropStatement:
    def test_the_drop_names_the_type_that_the_address_holds_now(self):
        address = TrouveAddress.parse(ADDRESS)
        # The drop must name the old type. DROP VIEW against a table raises
        # "Object found is of type 'TABLE', not specified type 'VIEW'".
        assert "DROP TABLE IF EXISTS analytics.revenue.orders" in (
            build_drop_physical_statement(ObjectType.TABLE, address)
        )
        assert "DROP VIEW IF EXISTS analytics.revenue.orders" in (
            build_drop_physical_statement(ObjectType.VIEW, address)
        )


class TestStagedRun:
    def test_a_table_that_becomes_a_view_drops_the_table_first(self):
        # RecordingAdapter answers TABLE for each address, thus the physical
        # address holds a table and the Trouve asks for a view.
        adapter = RecordingAdapter()
        result = _run(Trouve(type=TrouveType.VIEW, sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.SUCCESS
        executed = _executed_sql(result)
        drop_index = next(i for i, sql in enumerate(executed) if TYPE_CHANGE_DROP in sql)
        promote_index = next(i for i, sql in enumerate(executed) if PROMOTE_VIEW in sql)
        assert drop_index < promote_index
        assert f"DROP TABLE IF EXISTS {ADDRESS}" in executed[drop_index]

    def test_a_view_that_becomes_a_table_drops_the_view_first(self):
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        result = _run(Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.SUCCESS
        executed = _executed_sql(result)
        drop_index = next(i for i, sql in enumerate(executed) if TYPE_CHANGE_DROP in sql)
        promote_index = next(i for i, sql in enumerate(executed) if PROMOTE_TABLE in sql)
        assert drop_index < promote_index
        assert f"DROP VIEW IF EXISTS {ADDRESS}" in executed[drop_index]

    def test_a_type_that_does_not_change_sends_no_drop_of_the_physical_object(self):
        adapter = RecordingAdapter()
        result = _run(Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.SUCCESS
        # The plan drops the staging object, and it drops nothing else.
        drops = [sql for sql in _executed_sql(result) if "DROP" in sql]
        assert len(drops) == 1
        assert DROP_STAGING in drops[0]
        assert _staging_address() in drops[0]

    def test_a_type_change_waits_for_the_tests_to_pass(self):
        # The tests fail, thus the physical address keeps its object. A drop
        # here destroys the old data for a candidate that clair rejects.
        adapter = RecordingAdapter()
        result = _run(
            Trouve(type=TrouveType.VIEW, sql="SELECT 1 AS id"),
            adapter,
            tests_passed=False,
        )

        assert result.status == RunStatus.FAILURE
        assert not [sql for sql in _executed_sql(result) if TYPE_CHANGE_DROP in sql]

    def test_a_drop_that_fails_stops_the_node_and_keeps_the_candidate(self):
        adapter = RecordingAdapter(fail_on=["DROP TABLE IF EXISTS analytics"])
        result = _run(Trouve(type=TrouveType.VIEW, sql="SELECT 1 AS id"), adapter)

        assert result.status == RunStatus.FAILURE
        assert "cannot drop the object of the old type" in result.error
        assert _staging_address() in result.error
        # Clair sent no promotion after the drop failed.
        assert not [sql for sql in _executed_sql(result) if PROMOTE_VIEW in sql]

    def test_the_log_warns_that_the_grants_go_away(self):
        adapter = RecordingAdapter()
        with capture_logs() as log_entries:
            _run(Trouve(type=TrouveType.VIEW, sql="SELECT 1 AS id"), adapter)

        warnings = [
            e for e in log_entries if e["event"] == "run.node.object_type_changed"
        ]
        assert len(warnings) == 1
        assert warnings[0]["old_type"] == "table"
        assert warnings[0]["new_type"] == "view"
        assert "privilege" in warnings[0]["message"]


class TestRunWithNoStaging:
    def test_the_drop_comes_before_the_build(self):
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        result = _run(
            Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id"),
            adapter,
            use_staging=False,
        )

        assert result.status == RunStatus.SUCCESS
        executed = _executed_sql(result)
        assert "DROP VIEW IF EXISTS" in executed[0]
        assert f"CREATE OR REPLACE TABLE {ADDRESS}" in executed[1]

    def test_a_type_that_does_not_change_sends_no_drop(self):
        adapter = RecordingAdapter()
        result = _run(
            Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id"),
            adapter,
            use_staging=False,
        )

        assert result.status == RunStatus.SUCCESS
        assert not [sql for sql in _executed_sql(result) if "DROP" in sql]


class TestIncrementalFallback:
    def _incremental_trouve(self) -> Trouve:
        return Trouve(
            type=TrouveType.TABLE,
            sql="SELECT 1 AS id",
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL, incremental_mode=IncrementalMode.APPEND
            ),
        )

    def test_a_view_at_the_address_changes_the_run_to_a_full_refresh(self):
        # An incremental run needs a base table, and a view is not one. Clair
        # makes the table again, and the promotion drops the view.
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        with capture_logs() as log_entries:
            result = _run(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert result.status == RunStatus.SUCCESS
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert len(fallbacks) == 1
        assert fallbacks[0]["reason"] == "object_type_changed"
        # A clone of a view into a table is an error in Snowflake, thus the
        # plan must hold no seed clone. The promotion clones, and that is a
        # clone of the staging table, not of the view.
        assert not [sql for sql in _executed_sql(result) if SEED_CLONE in sql]
        drops = [sql for sql in _executed_sql(result) if TYPE_CHANGE_DROP in sql]
        assert len(drops) == 1
        assert f"DROP VIEW IF EXISTS {ADDRESS}" in drops[0]

    def test_an_address_that_holds_nothing_changes_the_run_to_a_full_refresh(self):
        adapter = RecordingAdapter(existing_tables=[])
        with capture_logs() as log_entries:
            result = _run(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert fallbacks[0]["reason"] == "object_not_found"

    def test_a_table_at_the_address_keeps_the_incremental_mode(self):
        adapter = RecordingAdapter()
        result = _run(self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.effective_run_mode == RunMode.INCREMENTAL
        assert [sql for sql in _executed_sql(result) if SEED_CLONE in sql]
        assert not [sql for sql in _executed_sql(result) if TYPE_CHANGE_DROP in sql]


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
    """The map from the TABLE_TYPE column of Snowflake onto ObjectType."""

    def test_a_base_table_is_a_table(self):
        adapter, _ = _snowflake_adapter_that_answers(("BASE TABLE",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == ObjectType.TABLE

    def test_a_view_is_a_view(self):
        adapter, _ = _snowflake_adapter_that_answers(("VIEW",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == ObjectType.VIEW

    def test_a_materialized_view_is_a_view(self):
        # Clair makes no materialized view, but a user can point a Trouve at an
        # address that holds one. A view is the closer answer of the two.
        adapter, _ = _snowflake_adapter_that_answers(("MATERIALIZED VIEW",))
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == ObjectType.VIEW

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
