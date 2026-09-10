"""Prove that a Trouve which changes its type repairs its address, in Snowflake.

Snowflake replaces an object with an object of the same type only. Against a
different type ``CREATE OR REPLACE`` raises ``Object '<name>' already exists.``
Clair therefore asks the warehouse for the type of the object at the physical
address, and it drops that object before it writes the new one.

These tests are the proof of the whole feature, because the fault lives in
Snowflake and no fake adapter shows it. Each test class builds one Trouve, runs
it, rewrites the same file with the other type, and runs it again.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair.adapters.base import ObjectType
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

pytestmark = pytest.mark.integration

LOGICAL_NAME_TEMPLATE = "{database_name}.refined.switched"


def _trouve_file(trouve_type: str) -> str:
    """Give the text of a Trouve file of one type. The SQL stays the same."""
    return (
        "from clair import Trouve, TrouveType\n"
        "\n"
        "trouve = Trouve(\n"
        f"    type=TrouveType.{trouve_type},\n"
        '    sql="SELECT 1 AS id, \'first\' AS label",\n'
        ")\n"
    )


def _write_project(destination: Path, database_name: str, trouve_type: str) -> Path:
    """Write a project that holds one Trouve, and give the path of its root."""
    project_path = destination / database_name
    trouve_directory = project_path / database_name / "refined"
    trouve_directory.mkdir(parents=True, exist_ok=True)

    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (project_path / database_name / "__database_config__.py").write_text(
        CI_DATABASE_CONFIG_FILE
    )
    (trouve_directory / "switched.py").write_text(_trouve_file(trouve_type))
    return project_path


def _rewrite_trouve(project_path: Path, database_name: str, trouve_type: str) -> None:
    """Change the type of the Trouve in place, and keep every other attribute."""
    trouve_file = project_path / database_name / "refined" / "switched.py"
    trouve_file.write_text(_trouve_file(trouve_type))


class _TypeChangeCase:
    """One run of each type, in the order that the subclass names."""

    DATABASE_NAME: str = ""
    FIRST_TYPE: str = ""
    SECOND_TYPE: str = ""

    @property
    def logical_name(self) -> str:
        return LOGICAL_NAME_TEMPLATE.format(database_name=self.DATABASE_NAME)

    @pytest.fixture(scope="class")
    def runs(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> tuple[RunSummary, RunSummary]:
        """Run the Trouve as the first type, then as the second type."""
        project_path = _write_project(
            tmp_path_factory.mktemp(self.DATABASE_NAME),
            self.DATABASE_NAME,
            self.FIRST_TYPE,
        )
        first = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)

        _rewrite_trouve(project_path, self.DATABASE_NAME, self.SECOND_TYPE)
        second = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        return first, second

    def test_the_first_run_makes_the_object_of_the_first_type(
        self,
        runs: tuple[RunSummary, RunSummary],
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        first, _ = runs
        result = first.result(self.logical_name)

        assert result is not None
        assert result.status == RunStatus.SUCCESS
        address = physical_address(self.logical_name, clair_environment.schema_name)
        assert adapter.object_type(address) == ObjectType[self.FIRST_TYPE]

    def test_the_second_run_replaces_it_with_the_other_type(
        self,
        runs: tuple[RunSummary, RunSummary],
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        """Without the drop, Snowflake raises "Object ... already exists"."""
        _, second = runs
        result = second.result(self.logical_name)

        assert result is not None, "the second run holds no result for the Trouve"
        assert result.status == RunStatus.SUCCESS, result.error
        address = physical_address(self.logical_name, clair_environment.schema_name)
        assert adapter.object_type(address) == ObjectType[self.SECOND_TYPE]

    def test_the_second_run_sends_the_drop_of_the_old_type(
        self, runs: tuple[RunSummary, RunSummary]
    ) -> None:
        _, second = runs
        result = second.result(self.logical_name)

        assert result is not None
        drops = [
            statement.sql
            for statement in result.statements
            if "the Trouve changed its type" in statement.sql
        ]
        assert len(drops) == 1, "clair sends the drop one time"
        assert f"DROP {self.FIRST_TYPE} IF EXISTS" in drops[0]


class TestATableThatBecomesAView(_TypeChangeCase):
    DATABASE_NAME = "type_change_table_to_view"
    FIRST_TYPE = "TABLE"
    SECOND_TYPE = "VIEW"


class TestAViewThatBecomesATable(_TypeChangeCase):
    DATABASE_NAME = "type_change_view_to_table"
    FIRST_TYPE = "VIEW"
    SECOND_TYPE = "TABLE"
