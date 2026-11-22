"""Prove what a Trouve that changes its type does, in Snowflake.

Snowflake replaces an object with an object of the same type only. Against a
different type ``CREATE OR REPLACE`` raises ``Object '<name>' already exists.``
Clair therefore drops the old object first. Clair supports future grants on the
schema, thus the new object gets its grants again.

These tests are the proof of the feature, because the fault lives in Snowflake
and no fake adapter shows it. Each test class runs one Trouve three times:

1. As the first type. The run makes the object.
2. As the second type, in a staged run. The promotion drops the old object, and
   makes the object of the second type.
3. As the first type again, with ``test=False``. The build drops the object of
   the second type before it writes.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import pytest

import clair
from clair.adapters.objects import WarehouseObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunSuccess, RunSummary
from clair.environments.environments import Environment
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    CI_DATABASE_CONFIG_FILE,
    CI_ROUTING_FILE,
    physical_address,
)
from tests.integration.warehouse import privileges_on

pytestmark = pytest.mark.integration

LOGICAL_NAME_TEMPLATE = "{database_name}.refined.switched"
TROUVE_CLASS_NAMES = {"TABLE": "Table", "VIEW": "View"}


def _trouve_file(trouve_type: str) -> str:
    """Give the text of a Trouve file of one type. The SQL stays the same."""
    class_name = TROUVE_CLASS_NAMES[trouve_type]
    return (
        f"from clair import {class_name}\n"
        f"trouve = {class_name}(\n"
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


class _TypeChangeRuns(NamedTuple):
    """The three runs, and the type and the privileges that the warehouse held after each one.

    The fixture runs all three runs before the first test starts. A test that
    asks the warehouse after the fixture sees the last state only, so the
    fixture reads each state at the time that it exists, and keeps it here.
    """

    first: RunSummary
    type_after_first_run: WarehouseObjectType | None
    second: RunSummary
    type_after_second_run: WarehouseObjectType | None
    privileges_after_second_run: set[str]
    third: RunSummary
    type_after_third_run: WarehouseObjectType | None
    privileges_after_third_run: set[str]


def _table_privileges(
    adapter: SnowflakeAdapter, address: TrouveAddress, object_type: WarehouseObjectType | None
) -> set[str]:
    """Give the privileges on a table. The future grant of the setup covers tables only."""
    if object_type != WarehouseObjectType.TABLE:
        return set()
    return privileges_on(adapter, address)


class _TypeChangeCase:
    """One Trouve that changes from FIRST_TYPE to SECOND_TYPE, and back."""

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
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> _TypeChangeRuns:
        """Run the first type, the second type staged, then the first type with no staging."""
        project_path = _write_project(
            tmp_path_factory.mktemp(self.DATABASE_NAME),
            self.DATABASE_NAME,
            self.FIRST_TYPE,
        )
        address = physical_address(self.logical_name, clair_environment.schema_name)

        first = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        type_after_first_run = adapter.object_type(address)

        _rewrite_trouve(project_path, self.DATABASE_NAME, self.SECOND_TYPE)
        second = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        type_after_second_run = adapter.object_type(address)
        privileges_after_second_run = _table_privileges(adapter, address, type_after_second_run)

        _rewrite_trouve(project_path, self.DATABASE_NAME, self.FIRST_TYPE)
        third = clair.run(
            project_path, env=environment, run_mode=RunMode.FULL_REFRESH, test=False
        )
        type_after_third_run = adapter.object_type(address)
        privileges_after_third_run = _table_privileges(adapter, address, type_after_third_run)

        return _TypeChangeRuns(
            first,
            type_after_first_run,
            second,
            type_after_second_run,
            privileges_after_second_run,
            third,
            type_after_third_run,
            privileges_after_third_run,
        )

    def test_the_first_run_makes_the_object_of_the_first_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        result = runs.first.result(self.logical_name)

        assert isinstance(result, RunSuccess), result
        assert runs.type_after_first_run == WarehouseObjectType[self.FIRST_TYPE]

    def test_a_staged_run_replaces_the_object_with_the_second_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        result = runs.second.result(self.logical_name)

        assert isinstance(result, RunSuccess), result
        assert runs.type_after_second_run == WarehouseObjectType[self.SECOND_TYPE]

    def test_a_run_with_no_staging_replaces_the_object_with_the_first_type_again(
        self, runs: _TypeChangeRuns
    ) -> None:
        result = runs.third.result(self.logical_name)

        assert isinstance(result, RunSuccess), result
        assert runs.type_after_third_run == WarehouseObjectType[self.FIRST_TYPE]

    def test_a_new_table_gets_the_future_grant(self, runs: _TypeChangeRuns) -> None:
        privileges_of_each_new_table = [
            privileges
            for privileges, object_type in (
                (runs.privileges_after_second_run, runs.type_after_second_run),
                (runs.privileges_after_third_run, runs.type_after_third_run),
            )
            if object_type == WarehouseObjectType.TABLE
        ]
        assert privileges_of_each_new_table, "no run made a table"
        assert all("SELECT" in privileges for privileges in privileges_of_each_new_table)


class TestATableThatBecomesAView(_TypeChangeCase):
    DATABASE_NAME = "type_change_table_to_view"
    FIRST_TYPE = "TABLE"
    SECOND_TYPE = "VIEW"


class TestAViewThatBecomesATable(_TypeChangeCase):
    DATABASE_NAME = "type_change_view_to_table"
    FIRST_TYPE = "VIEW"
    SECOND_TYPE = "TABLE"
