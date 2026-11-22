"""The tests of a Trouve that changes between a TABLE, an ICEBERG TABLE and a VIEW.

Snowflake replaces an object with an object of the same type only, thus clair
drops the old object first. Clair supports future grants on the schema, so the
new object gets its grants again. A staged run builds and tests the candidate,
and drops the old object only when it promotes. A run with no staging drops it
before its first write.
"""

from __future__ import annotations

import pandas as pd
import pytest
from structlog.testing import capture_logs

from clair.adapters.objects import WarehouseObject, WarehouseObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunFailure, RunSuccess
from clair.core.staging import make_staging_address
from clair.environments.routing import TrouveAddress
from clair.exceptions import RunError
from clair.trouves.pandas_table import PandasTable
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.seed_table import SeedTable
from clair.trouves.trouve import (
    Table,
    View,
)
from tests.helpers import (
    TEST_RUN_ID,
    RecordingAdapter,
    executed_sql,
    run_one_trouve,
    snowflake_adapter_that_answers,
)

RUN_ID = TEST_RUN_ID
ADDRESS = "analytics.revenue.orders"

# The staging address starts with the physical address, thus a test that looks
# for a physical statement by its address alone also finds the staging one.
# Each statement carries a comment that names its step, so the tests match that.
SEED_CLONE = "clone the target"
PROMOTE = "-- staging: promote the tested"


def _staging_address(address: str = ADDRESS) -> str:
    return str(make_staging_address(TrouveAddress.parse(address), RUN_ID))


def _index_of_drop(result: RunSuccess | RunFailure, old_type_name: str) -> int:
    """Give the position of the DROP of the old object, among the statements that clair sent."""
    (drop_index,) = [
        index
        for index, sql in enumerate(executed_sql(result))
        if sql.endswith(f"DROP {old_type_name} IF EXISTS {ADDRESS}")
    ]
    return drop_index


def _drops_of_the_physical_object(result: RunSuccess | RunFailure) -> list[str]:
    """Give each statement that drops the physical address, and not the staging one."""
    return [
        sql
        for sql in executed_sql(result)
        if "DROP" in sql and _staging_address() not in sql
    ]


def _dataframe_trouves() -> list[PandasTable | SeedTable]:
    return [
        PandasTable(transform=lambda: pd.DataFrame({"id": [1]})),
        SeedTable(dataframe=pd.DataFrame({"id": [1]})),
    ]


# Each case: the Trouve, an adapter whose physical address holds the old type,
# and the name of the old type.
TYPE_CHANGES = [
    pytest.param(View(sql="SELECT 1 AS id"), lambda: RecordingAdapter(), "TABLE", id="table to view"),
    pytest.param(
        Table(sql="SELECT 1 AS id"),
        lambda: RecordingAdapter(existing_views=[ADDRESS]),
        "VIEW",
        id="view to table",
    ),
    pytest.param(
        _dataframe_trouves()[0],
        lambda: RecordingAdapter(existing_views=[ADDRESS]),
        "VIEW",
        id="view to pandas table",
    ),
    pytest.param(
        _dataframe_trouves()[1],
        lambda: RecordingAdapter(existing_views=[ADDRESS]),
        "VIEW",
        id="view to seed table",
    ),
]


class TestWantedObjectType:
    def test_a_table_trouve_wants_a_table(self):
        assert Table(sql="SELECT 1 AS id").warehouse_object_type == WarehouseObjectType.TABLE

    def test_a_view_trouve_wants_a_view(self):
        assert View(sql="SELECT 1 AS id").warehouse_object_type == WarehouseObjectType.VIEW


class TestStagedRun:
    @pytest.mark.parametrize(("trouve", "make_adapter", "old_type_name"), TYPE_CHANGES)
    def test_the_promotion_drops_the_old_object_then_makes_the_new_one(
        self, trouve: Table | View | PandasTable | SeedTable, make_adapter, old_type_name: str
    ):
        result = run_one_trouve(trouve, make_adapter())

        assert isinstance(result, RunSuccess), result
        statements = executed_sql(result)
        (promote_index,) = [index for index, sql in enumerate(statements) if PROMOTE in sql]
        assert _index_of_drop(result, old_type_name) == promote_index - 1

    @pytest.mark.parametrize(("trouve", "make_adapter", "old_type_name"), TYPE_CHANGES)
    def test_a_failed_test_drops_nothing(
        self, trouve: Table | View | PandasTable | SeedTable, make_adapter, old_type_name: str
    ):
        # The old object stays in production until a candidate passes its tests.
        result = run_one_trouve(trouve, make_adapter(), tests_passed=False)

        assert isinstance(result, RunFailure)
        assert "the tests failed" in result.error
        assert _drops_of_the_physical_object(result) == []

    def test_a_failed_promotion_after_the_drop_says_that_production_holds_nothing(self):
        adapter = RecordingAdapter(existing_views=[ADDRESS], fail_on=["promote the tested table"])
        result = run_one_trouve(Table(sql="SELECT 1 AS id"), adapter)

        assert isinstance(result, RunFailure)
        assert f"{ADDRESS} holds no object now" in result.error
        assert f"The candidate stays at {_staging_address()}" in result.error

    @pytest.mark.parametrize(
        ("trouve", "adapter"),
        [
            (Table(sql="SELECT 1 AS id"), RecordingAdapter()),
            (View(sql="SELECT 1 AS id"), RecordingAdapter(existing_tables=[])),
        ],
        ids=["same type", "no object"],
    )
    def test_an_object_of_the_same_type_or_none_gets_no_drop(self, trouve, adapter):
        result = run_one_trouve(trouve, adapter)

        assert isinstance(result, RunSuccess)
        assert _drops_of_the_physical_object(result) == []


class TestRunWithNoStaging:
    @pytest.mark.parametrize(("trouve", "make_adapter", "old_type_name"), TYPE_CHANGES)
    def test_the_drop_comes_before_the_first_write(
        self, trouve: Table | View | PandasTable | SeedTable, make_adapter, old_type_name: str
    ):
        adapter = make_adapter()
        result = run_one_trouve(trouve, adapter, use_staging=False)

        assert isinstance(result, RunSuccess), result
        assert _index_of_drop(result, old_type_name) == 0
        if adapter.written_addresses:
            assert adapter.written_addresses == [ADDRESS]

    def test_a_type_that_does_not_change_builds_with_no_drop(self):
        result = run_one_trouve(Table(sql="SELECT 1 AS id"), RecordingAdapter(), use_staging=False)

        assert isinstance(result, RunSuccess)
        assert f"CREATE OR REPLACE TABLE {ADDRESS}" in executed_sql(result)[0]


class TestOneReadForEachNode:
    """The runner reads the physical object one time, and each test of the node uses that answer."""

    @pytest.mark.parametrize("use_staging", [True, False], ids=["staged", "no staging"])
    @pytest.mark.parametrize(
        ("trouve", "run_mode"),
        [
            (Table(sql="SELECT 1 AS id"), RunMode.FULL_REFRESH),
            (
                Table(sql="SELECT 1 AS id", run_config=Incremental(strategy=Append())),
                RunMode.INCREMENTAL,
            ),
            (View(sql="SELECT 1 AS id"), RunMode.FULL_REFRESH),
            (PandasTable(transform=lambda: pd.DataFrame({"id": [1]})), RunMode.FULL_REFRESH),
        ],
        ids=["full refresh", "incremental", "view", "pandas"],
    )
    def test_each_path_describes_the_physical_object_one_time(
        self, trouve: Table | View | PandasTable, run_mode: RunMode, use_staging: bool
    ):
        adapter = RecordingAdapter(existing_views=[ADDRESS] if isinstance(trouve, View) else [])

        result = run_one_trouve(trouve, adapter, run_mode=run_mode, use_staging=use_staging)

        assert isinstance(result, RunSuccess), result
        assert adapter.described_addresses == [ADDRESS]
class TestIncrementalFallback:
    def _incremental_trouve(self) -> Table:
        return Table(
            sql="SELECT 1 AS id",
            run_config=Incremental(strategy=Append()),
        )

    def test_a_view_at_the_address_builds_a_full_table_and_replaces_the_view(self):
        # An incremental run needs a base table, and a view is not one. Clair
        # builds the full table as the candidate, tests it, drops the view, and
        # promotes the table.
        adapter = RecordingAdapter(existing_views=[ADDRESS])
        with capture_logs() as log_entries:
            result = run_one_trouve(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert isinstance(result, RunSuccess), result
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert len(fallbacks) == 1
        assert fallbacks[0]["reason"] == "object_type_changed"
        # A clone of a view into a table is an error in Snowflake, thus the
        # plan must hold no seed clone.
        assert not [sql for sql in executed_sql(result) if SEED_CLONE in sql]
        assert _index_of_drop(result, "VIEW") >= 0

    def test_an_address_that_holds_nothing_changes_the_run_to_a_full_refresh(self):
        adapter = RecordingAdapter(existing_tables=[])
        with capture_logs() as log_entries:
            result = run_one_trouve(
                self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL
            )

        assert isinstance(result, RunSuccess)
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        fallbacks = [
            e for e in log_entries if e["event"] == "run.node.incremental_fallback"
        ]
        assert fallbacks[0]["reason"] == "object_not_found"

    def test_a_table_at_the_address_keeps_the_incremental_mode(self):
        adapter = RecordingAdapter()
        result = run_one_trouve(self._incremental_trouve(), adapter, run_mode=RunMode.INCREMENTAL)

        assert isinstance(result, RunSuccess)
        assert result.effective_run_mode == RunMode.INCREMENTAL
        assert [sql for sql in executed_sql(result) if SEED_CLONE in sql]


class TestSnowflakeObjectType:
    """The map from the TABLE_TYPE column of Snowflake onto WarehouseObjectType."""

    def test_a_base_table_is_a_table(self):
        adapter, _ = snowflake_adapter_that_answers([("BASE TABLE", "NO", None)])
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.TABLE

    def test_a_view_is_a_view(self):
        adapter, _ = snowflake_adapter_that_answers([("VIEW", "NO", None)])
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.VIEW

    def test_a_materialized_view_is_a_view(self):
        # Clair makes no materialized view, but a user can point a Trouve at an
        # address that holds one. A view is the closer answer of the two.
        adapter, _ = snowflake_adapter_that_answers([("MATERIALIZED VIEW", "NO", None)])
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.VIEW

    def test_an_iceberg_base_table_is_an_iceberg_table(self):
        adapter, _ = snowflake_adapter_that_answers([("BASE TABLE", "YES", None)])
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) == WarehouseObjectType.ICEBERG_TABLE

    def test_the_comment_of_the_object_comes_with_its_type(self):
        adapter, _ = snowflake_adapter_that_answers([("BASE TABLE", "NO", "The orders.")])
        assert adapter.describe_object(TrouveAddress.parse(ADDRESS)) == WarehouseObject(
            object_type=WarehouseObjectType.TABLE, comment="The orders."
        )

    def test_no_row_means_the_address_holds_nothing(self):
        adapter, _ = snowflake_adapter_that_answers([])
        assert adapter.object_type(TrouveAddress.parse(ADDRESS)) is None

    def test_a_query_that_fails_raises_and_never_means_no_object(self):
        # No object makes an incremental Trouve build its table again, thus a
        # query that fails must not look like an empty address.
        adapter, cursor = snowflake_adapter_that_answers(
            [], error=RuntimeError("Network unreachable")
        )
        with pytest.raises(RunError, match="Network unreachable"):
            adapter.object_type(TrouveAddress.parse(ADDRESS))
        assert cursor.closed

    def test_the_query_reads_the_information_schema_of_the_database(self):
        adapter, cursor = snowflake_adapter_that_answers([("BASE TABLE", "NO", None)])
        adapter.object_type(TrouveAddress.parse(ADDRESS))
        assert "analytics.INFORMATION_SCHEMA.TABLES" in cursor.sql
        assert "TABLE_SCHEMA = 'REVENUE'" in cursor.sql
        assert "TABLE_NAME = 'ORDERS'" in cursor.sql

    def test_an_adapter_with_no_connection_raises(self):
        adapter = SnowflakeAdapter()
        with pytest.raises(RuntimeError, match="Not connected"):
            adapter.object_type(TrouveAddress.parse(ADDRESS))
