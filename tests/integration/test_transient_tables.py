"""Prove the transient option against Snowflake, and the change of kind in each direction.

A mock adapter shows the keyword in the SQL. Only the warehouse shows the
kind of the table that the statements make, and which change of kind keeps the
rows and the grants. Snowflake clones a permanent table into a transient one,
but it does not clone a transient table into a permanent one.

The fixture runs one incremental Trouve four times:

1. Permanent. The table is permanent, and the test grants INSERT on it.
2. Transient, incremental. The clone makes the table transient. It keeps the
   rows and the grant.
3. Permanent, incremental. The clone of a transient table into a permanent one
   fails. The table keeps its kind and its rows.
4. Permanent, full refresh. The table becomes permanent, from the SQL.
"""

from __future__ import annotations

from typing import NamedTuple

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import execute, privileges_on, query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "transient_tables"
LOGICAL_NAME = f"{DATABASE_NAME}.refined.events"


def incremental_file(new_id: int, transient: bool) -> str:
    return (
        "from clair import Append, Incremental, Table\n"
        "trouve = Table(\n"
        f'    sql="SELECT {new_id} AS id",\n'
        "    run_config=Incremental(strategy=Append()),\n"
        f"    transient={transient},\n"
        ")\n"
    )


def is_transient_table(adapter: SnowflakeAdapter, address: TrouveAddress) -> bool:
    """Read the kind of one table from INFORMATION_SCHEMA."""
    rows = query_rows(
        adapter,
        f"SELECT IS_TRANSIENT FROM {address.database_name}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{address.schema_name.upper()}' "
        f"AND TABLE_NAME = '{address.table_name.upper()}'",
    )
    assert len(rows) == 1, rows
    return rows[0][0] == "YES"


def ids_of(adapter: SnowflakeAdapter, address: TrouveAddress) -> list[object]:
    return [row[0] for row in query_rows(adapter, f"SELECT id FROM {address} ORDER BY id")]


class _Step(NamedTuple):
    summary: RunSummary
    transient: bool
    ids: list[object]
    privileges: set[str]


class _Runs(NamedTuple):
    permanent: _Step
    to_transient: _Step
    to_permanent_incremental: _Step
    to_permanent_full_refresh: _Step


class TestAChangeOfKind:
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

        def run(new_id: int, transient: bool, run_mode: RunMode) -> _Step:
            project_path = write_refined_project(
                destination, DATABASE_NAME, {"events": incremental_file(new_id, transient)}
            )
            summary = clair.run(project_path, env=environment, run_mode=run_mode)
            return _Step(
                summary,
                is_transient_table(adapter, address),
                ids_of(adapter, address),
                privileges_on(adapter, address),
            )

        permanent = run(1, transient=False, run_mode=RunMode.INCREMENTAL)
        # No future grant covers INSERT, thus only COPY GRANTS can keep it.
        execute(adapter, f"GRANT INSERT ON TABLE {address} TO ROLE {clair_environment.role}")
        to_transient = run(2, transient=True, run_mode=RunMode.INCREMENTAL)
        to_permanent_incremental = run(3, transient=False, run_mode=RunMode.INCREMENTAL)
        to_permanent_full_refresh = run(3, transient=False, run_mode=RunMode.FULL_REFRESH)
        return _Runs(permanent, to_transient, to_permanent_incremental, to_permanent_full_refresh)

    def test_a_table_is_permanent_by_default(self, runs: _Runs) -> None:
        assert runs.permanent.summary.failed == []
        assert not runs.permanent.transient

    def test_an_incremental_run_makes_a_permanent_table_transient(self, runs: _Runs) -> None:
        result = runs.to_transient.summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.SUCCESS, result.error
        assert runs.to_transient.transient

    def test_the_change_to_transient_keeps_the_rows_and_the_grants(self, runs: _Runs) -> None:
        assert runs.to_transient.ids == [1, 2]
        assert "INSERT" in runs.to_transient.privileges

    def test_an_incremental_run_cannot_make_a_transient_table_permanent(self, runs: _Runs) -> None:
        result = runs.to_permanent_incremental.summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.FAILURE
        assert "cannot be cloned to a permanent object" in result.error

    def test_the_failed_change_leaves_the_table_as_it_was(self, runs: _Runs) -> None:
        assert runs.to_permanent_incremental.transient
        assert runs.to_permanent_incremental.ids == [1, 2]

    def test_a_full_refresh_makes_the_table_permanent_and_keeps_the_grants(self, runs: _Runs) -> None:
        result = runs.to_permanent_full_refresh.summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.SUCCESS, result.error
        assert not runs.to_permanent_full_refresh.transient
        assert runs.to_permanent_full_refresh.ids == [3]
        assert "INSERT" in runs.to_permanent_full_refresh.privileges


SEED_FILE = """\
import pandas as pd
from clair import SeedTable
trouve = SeedTable(dataframe=pd.DataFrame({"id": [1, 2]}), transient=True)
"""

PANDAS_FILE = f"""\
import pandas as pd
from clair import PandasTable
from {DATABASE_NAME}_pandas.refined.seed import trouve as seed


def double(seed: pd.DataFrame) -> pd.DataFrame:
    return seed.assign(id=seed["id"] * 2)


trouve = PandasTable(transform=double, inputs=[seed], transient=True)
"""


class TestADataframeTrouve:
    DATABASE_NAME = f"{DATABASE_NAME}_pandas"

    @pytest.fixture(scope="class")
    def summary(
        self, environment: Environment, tmp_path_factory: pytest.TempPathFactory
    ) -> RunSummary:
        project_path = write_refined_project(
            tmp_path_factory.mktemp(self.DATABASE_NAME),
            self.DATABASE_NAME,
            {"seed": SEED_FILE, "doubled": PANDAS_FILE},
        )
        return clair.run(project_path, env=environment)

    @pytest.mark.parametrize("table_name", ["seed", "doubled"])
    def test_a_transient_dataframe_trouve_makes_a_transient_table(
        self,
        summary: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        table_name: str,
    ) -> None:
        assert summary.failed == [], [result.error for result in summary.failed]
        address = physical_address(
            f"{self.DATABASE_NAME}.refined.{table_name}", clair_environment.schema_name
        )
        assert is_transient_table(adapter, address)
