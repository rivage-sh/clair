"""Prove the Iceberg option against Snowflake: the build, the clone, the promotion and a change of kind.

The tests give no external volume, thus Snowflake takes the default of the
account. Without a default, it uses ``SNOWFLAKE_MANAGED``.

The fixture runs one incremental Trouve three times:

1. Iceberg, full refresh. The table is an Iceberg table, with the rows of the SQL.
2. Iceberg, incremental. The clone and the promotion keep the kind, and the
   table keeps its rows.
3. Standard, full refresh. Snowflake cannot replace an Iceberg table with a
   standard table, thus the promotion stops and the table stays as it was.
"""

from __future__ import annotations

from typing import NamedTuple

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.base import WarehouseObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "iceberg_tables"
LOGICAL_NAME = f"{DATABASE_NAME}.refined.events"


def events_file(new_id: int, iceberg: bool) -> str:
    iceberg_argument = "    iceberg=Iceberg(),\n" if iceberg else ""
    return (
        "from clair import Append, Iceberg, Incremental, Table\n"
        "trouve = Table(\n"
        f'    sql="SELECT {new_id} AS id",\n'
        "    run_config=Incremental(strategy=Append()),\n"
        f"{iceberg_argument}"
        ")\n"
    )


def ids_of(adapter: SnowflakeAdapter, address: TrouveAddress) -> list[object]:
    return [row[0] for row in query_rows(adapter, f"SELECT id FROM {address} ORDER BY id")]


class _Step(NamedTuple):
    summary: RunSummary
    object_type: WarehouseObjectType | None
    ids: list[object]


class _Runs(NamedTuple):
    full_refresh: _Step
    incremental: _Step
    to_standard: _Step


class TestAnIcebergTable:
    @pytest.fixture(scope="class")
    def runs(
        self,
        environment: Environment,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> _Runs:
        destination = tmp_path_factory.mktemp(DATABASE_NAME)
        address = physical_address(LOGICAL_NAME, clair_environment.schema_name)

        def run(new_id: int, iceberg: bool, run_mode: RunMode) -> _Step:
            project_path = write_refined_project(
                destination, DATABASE_NAME, {"events": events_file(new_id, iceberg)}
            )
            summary = clair.run(project_path, env=environment, run_mode=run_mode)
            return _Step(summary, adapter.object_type(address), ids_of(adapter, address))

        return _Runs(
            full_refresh=run(1, iceberg=True, run_mode=RunMode.FULL_REFRESH),
            incremental=run(2, iceberg=True, run_mode=RunMode.INCREMENTAL),
            to_standard=run(3, iceberg=False, run_mode=RunMode.FULL_REFRESH),
        )

    def test_a_full_refresh_makes_an_iceberg_table(self, runs: _Runs) -> None:
        assert runs.full_refresh.summary.failed == [], [
            result.error for result in runs.full_refresh.summary.failed
        ]
        assert runs.full_refresh.object_type == WarehouseObjectType.ICEBERG_TABLE
        assert runs.full_refresh.ids == [1]

    def test_an_incremental_run_clones_and_promotes_an_iceberg_table(self, runs: _Runs) -> None:
        result = runs.incremental.summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.SUCCESS, result.error
        assert result.effective_run_mode == RunMode.INCREMENTAL
        assert runs.incremental.object_type == WarehouseObjectType.ICEBERG_TABLE
        assert runs.incremental.ids == [1, 2]

    def test_a_change_to_a_standard_table_stops_and_keeps_the_iceberg_table(
        self, runs: _Runs
    ) -> None:
        result = runs.to_standard.summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.FAILURE
        assert "Run DROP ICEBERG TABLE" in result.error
        assert runs.to_standard.object_type == WarehouseObjectType.ICEBERG_TABLE
        assert runs.to_standard.ids == [1, 2]
