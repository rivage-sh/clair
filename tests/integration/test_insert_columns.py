"""Prove that an incremental insert matches each column by its name, in Snowflake.

The second run moves two text columns into the opposite order. An INSERT with
no column list would swap their values, and Snowflake would give no error. The
values must land in their own columns.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    CI_DATABASE_CONFIG_FILE,
    CI_ROUTING_FILE,
    physical_address,
)
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "insert_columns"
FIRST_SQL = "SELECT 1 AS id, 'first label' AS label, 'first code' AS code"
REORDERED_SQL = "SELECT 2 AS id, 'second code' AS code, 'second label' AS label"
# Each logical name, and the strategy of its Trouve.
STRATEGIES = {
    f"{DATABASE_NAME}.refined.appended": "Append()",
    f"{DATABASE_NAME}.refined.delete_inserted": 'DeleteInsert(primary_key_columns=["id"])',
}


def trouve_file(sql: str, strategy: str) -> str:
    return (
        "from clair import Append, DeleteInsert, Incremental, Table\n"
        f'trouve = Table(sql="{sql}", run_config=Incremental(strategy={strategy}))\n'
    )


def write_project(destination: Path, sql: str) -> Path:
    """Write one Trouve for each strategy, each with the same SQL."""
    project_path = destination / DATABASE_NAME
    trouve_directory = project_path / DATABASE_NAME / "refined"
    trouve_directory.mkdir(parents=True, exist_ok=True)
    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (project_path / DATABASE_NAME / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)
    for logical_name, strategy in STRATEGIES.items():
        table_name = logical_name.rsplit(".", 1)[1]
        (trouve_directory / f"{table_name}.py").write_text(trouve_file(sql, strategy))
    return project_path


class TestAReorderedSelect:
    @pytest.fixture(scope="class")
    def reordered_run(
        self,
        environment: Environment,
        tmp_path_factory: pytest.TempPathFactory,
        workspace_prefix: str,
    ) -> RunSummary:
        """Build each table, then run it incrementally with the columns in a new order."""
        destination = tmp_path_factory.mktemp(DATABASE_NAME)
        project_path = write_project(destination, FIRST_SQL)
        first = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        assert first.failed_count == 0, [result.error for result in first.failed]

        write_project(destination, REORDERED_SQL)
        return clair.run(project_path, env=environment, run_mode=RunMode.INCREMENTAL)

    @pytest.mark.parametrize("logical_name", list(STRATEGIES))
    def test_each_value_lands_in_its_own_column(
        self,
        logical_name: str,
        reordered_run: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        result = reordered_run.result(logical_name)
        assert result.status == RunStatus.SUCCESS, result.error
        assert result.effective_run_mode == RunMode.INCREMENTAL

        address = physical_address(logical_name, clair_environment.schema_name)
        rows = query_rows(adapter, f"SELECT id, label, code FROM {address} ORDER BY id")
        assert rows == [(1, "first label", "first code"), (2, "second label", "second code")]
