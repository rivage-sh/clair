"""Prove against Snowflake that a promoted table keeps its clustering key, and Automatic Clustering.

A mock adapter shows the statements. Only the warehouse shows that a clone
suspends Automatic Clustering, and that ``RESUME RECLUSTER`` turns it on again.

The fixture runs one incremental Trouve three times:

1. Full refresh, key ``id``. The CREATE TABLE AS SELECT gives no key, and the
   ALTER on the staging table gives it.
2. Incremental, key ``id``. The staging table is a clone of the physical table.
3. Incremental, key ``id, name``. The ALTER replaces the key that the clone
   copied.
"""

from __future__ import annotations

from typing import NamedTuple

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunSummary
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import query_rows

pytestmark = pytest.mark.integration

DATABASE_NAME = "cluster_by"
LOGICAL_NAME = f"{DATABASE_NAME}.refined.events"


def incremental_file(new_id: int, cluster_by: list[str]) -> str:
    return (
        "from clair import Append, Incremental, Table\n"
        "trouve = Table(\n"
        f"    sql=\"SELECT {new_id} AS id, 'event' AS name\",\n"
        "    run_config=Incremental(strategy=Append()),\n"
        f"    cluster_by={cluster_by!r},\n"
        ")\n"
    )


class _Clustering(NamedTuple):
    key: str
    automatic_clustering: bool


def clustering_of(adapter: SnowflakeAdapter, address: TrouveAddress) -> _Clustering:
    """Read the clustering key and the Automatic Clustering state from INFORMATION_SCHEMA."""
    rows = query_rows(
        adapter,
        f"SELECT CLUSTERING_KEY, AUTO_CLUSTERING_ON "
        f"FROM {address.database_name}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{address.schema_name.upper()}' "
        f"AND TABLE_NAME = '{address.table_name.upper()}'",
    )
    assert len(rows) == 1, rows
    key, automatic_clustering = rows[0]
    # The documentation gives AUTO_CLUSTERING_ON as a BOOLEAN, and IS_TRANSIENT
    # as YES or NO. Accept both forms.
    return _Clustering(str(key or "").upper(), automatic_clustering in (True, "YES"))


class _Step(NamedTuple):
    summary: RunSummary
    clustering: _Clustering


class _Runs(NamedTuple):
    full_refresh: _Step
    incremental: _Step
    new_key: _Step


class TestAPromotedTable:
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

        def run(new_id: int, cluster_by: list[str], run_mode: RunMode) -> _Step:
            project_path = write_refined_project(
                destination, DATABASE_NAME, {"events": incremental_file(new_id, cluster_by)}
            )
            summary = clair.run(project_path, env=environment, run_mode=run_mode)
            return _Step(summary, clustering_of(adapter, address))

        return _Runs(
            full_refresh=run(1, ["id"], RunMode.FULL_REFRESH),
            incremental=run(2, ["id"], RunMode.INCREMENTAL),
            new_key=run(3, ["id", "name"], RunMode.INCREMENTAL),
        )

    @pytest.mark.parametrize("step_name", ["full_refresh", "incremental", "new_key"])
    def test_each_run_leaves_the_key_and_automatic_clustering_on(
        self, runs: _Runs, step_name: str
    ) -> None:
        step: _Step = getattr(runs, step_name)
        assert step.summary.failed == [], [result.error for result in step.summary.failed]
        assert "ID" in step.clustering.key
        assert step.clustering.automatic_clustering

    def test_an_incremental_run_replaces_the_key_that_the_clone_copied(self, runs: _Runs) -> None:
        assert "NAME" not in runs.incremental.clustering.key
        assert "NAME" in runs.new_key.clustering.key


SEED_FILE = """\
import pandas as pd
from clair import SeedTable
trouve = SeedTable(dataframe=pd.DataFrame({"id": [1, 2]}), cluster_by=["id"])
"""


def test_a_seed_table_keeps_its_key_and_automatic_clustering(
    environment: Environment,
    adapter: SnowflakeAdapter,
    clair_environment: IntegrationConfig,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    database_name = f"{DATABASE_NAME}_seed"
    project_path = write_refined_project(
        tmp_path_factory.mktemp(database_name), database_name, {"seed": SEED_FILE}
    )

    summary = clair.run(project_path, env=environment)

    assert summary.failed == [], [result.error for result in summary.failed]
    address = physical_address(f"{database_name}.refined.seed", clair_environment.schema_name)
    clustering = clustering_of(adapter, address)
    assert "ID" in clustering.key
    assert clustering.automatic_clustering
