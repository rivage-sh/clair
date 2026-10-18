"""Prove that a second run which changes the table stops the promotion, in Snowflake.

The adapter of the test commits one row to the physical table between the
clone and the promotion, in the same way as a second run of the same Trouve.
SYSTEM$LAST_CHANGE_COMMIT_TIME must change, thus clair must not promote, and
the row of the second run must stay.
"""

from __future__ import annotations

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.environments.environments import Environment
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "concurrent_runs"
LOGICAL_NAME = f"{DATABASE_NAME}.refined.appended"
TROUVE_FILE = (
    "from clair import Append, Incremental, Table\n"
    'trouve = Table(sql="SELECT 1 AS id", run_config=Incremental(strategy=Append()))\n'
)


class SecondRunAdapter(SnowflakeAdapter):
    """An adapter that commits one row to the table on the second read of its change token.

    The runner reads the token before the clone and before the promotion. The
    row thus lands between the two, as the row of a second run does.
    """

    def __init__(self, table_address: TrouveAddress) -> None:
        super().__init__()
        self.table_address = table_address
        self.token_read_count = 0

    def change_token(self, address: TrouveAddress) -> str | None:
        self.token_read_count += 1
        if self.token_read_count == 2:
            inserted = self.execute(f"INSERT INTO {self.table_address} SELECT 9 AS id")
            assert inserted.success, inserted.error
        return super().change_token(address)


class TestASecondRun:
    @pytest.fixture(scope="class")
    def runs(
        self,
        environment: Environment,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
        workspace_prefix: str,
    ) -> tuple[RunSummary, TrouveAddress]:
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DATABASE_NAME), DATABASE_NAME, {"appended": TROUVE_FILE}
        )
        first = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        assert first.failed_count == 0, [result.error for result in first.failed]

        address = physical_address(LOGICAL_NAME, clair_environment.schema_name)
        adapter = SecondRunAdapter(address)
        adapter.connect(environment)
        try:
            interrupted = clair.run(
                project_path, env=environment, run_mode=RunMode.INCREMENTAL, threads=1, adapter=adapter
            )
        finally:
            adapter.close()
        return interrupted, address

    def test_the_run_does_not_promote(self, runs: tuple[RunSummary, TrouveAddress]) -> None:
        summary, _ = runs
        result = summary.result(LOGICAL_NAME)
        assert result.status == RunStatus.FAILURE
        assert "another run changed" in result.error

    def test_the_row_of_the_second_run_stays(
        self, runs: tuple[RunSummary, TrouveAddress], adapter: SnowflakeAdapter
    ) -> None:
        _, address = runs
        assert query_rows(adapter, f"SELECT id FROM {address} ORDER BY id") == [(1,), (9,)]
