"""Prove what a Trouve that changes its type does, in Snowflake.

Snowflake replaces an object with an object of the same type only. Against a
different type ``CREATE OR REPLACE`` raises ``Object '<name>' already exists.``
Clair does not drop the old object, because a drop removes each grant on it.
The run builds and tests the candidate, stops before the promotion, and names
the drop that a person must run.

These tests are the proof of the feature, because the fault lives in Snowflake
and no fake adapter shows it. Each test class runs one Trouve three times:

1. As the first type. The run makes the object.
2. As the second type. The run stops, and the object keeps the first type.
3. As the second type again, after the test runs the drop from the error. The
   address is empty, thus the run makes the object of the second type.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import pytest

import clair
from clair.adapters.base import ObjectType
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.core.staging import make_staging_address
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    CI_DATABASE_CONFIG_FILE,
    CI_ROUTING_FILE,
    physical_address,
)
from tests.integration.warehouse import execute

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


class _TypeChangeRuns(NamedTuple):
    """The three runs, and the types that the warehouse held between them.

    The fixture runs all three runs before the first test starts. A test that
    asks the warehouse after the fixture sees the last state only, so the
    fixture reads each state at the time that it exists, and keeps it here.
    """

    first: RunSummary
    type_after_first_run: ObjectType | None
    second: RunSummary
    type_after_second_run: ObjectType | None
    candidate_type_after_second_run: ObjectType | None
    third: RunSummary
    type_after_third_run: ObjectType | None


class _TypeChangeCase:
    """One Trouve that changes from FIRST_TYPE to SECOND_TYPE."""

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
        """Run the first type, change the type, run, drop by hand, run again."""
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
        candidate_type_after_second_run = adapter.object_type(
            make_staging_address(address, second.run_id)
        )

        # The person reads the error and runs the drop that it names.
        execute(adapter, f"DROP {self.FIRST_TYPE} {address}")
        third = clair.run(project_path, env=environment, run_mode=RunMode.FULL_REFRESH)
        type_after_third_run = adapter.object_type(address)

        return _TypeChangeRuns(
            first,
            type_after_first_run,
            second,
            type_after_second_run,
            candidate_type_after_second_run,
            third,
            type_after_third_run,
        )

    def test_the_first_run_makes_the_object_of_the_first_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        result = runs.first.result(self.logical_name)

        assert result is not None
        assert result.status == RunStatus.SUCCESS, result.error
        assert runs.type_after_first_run == ObjectType[self.FIRST_TYPE]

    def test_the_second_run_fails_and_names_the_drop(
        self, runs: _TypeChangeRuns, clair_environment: IntegrationConfig
    ) -> None:
        result = runs.second.result(self.logical_name)
        address = physical_address(self.logical_name, clair_environment.schema_name)

        assert result is not None, "the second run holds no result for the Trouve"
        assert result.status == RunStatus.FAILURE
        assert f"Run DROP {self.FIRST_TYPE} {address}" in result.error

    def test_the_second_run_keeps_the_object_of_the_first_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        """Clair sends no drop, so each grant on the object stays."""
        assert runs.type_after_second_run == ObjectType[self.FIRST_TYPE]

    def test_the_second_run_keeps_a_tested_candidate_of_the_second_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        assert runs.candidate_type_after_second_run == ObjectType[self.SECOND_TYPE]

    def test_after_the_drop_the_next_run_makes_the_second_type(
        self, runs: _TypeChangeRuns
    ) -> None:
        result = runs.third.result(self.logical_name)

        assert result is not None
        assert result.status == RunStatus.SUCCESS, result.error
        assert runs.type_after_third_run == ObjectType[self.SECOND_TYPE]


class TestATableThatBecomesAView(_TypeChangeCase):
    DATABASE_NAME = "type_change_table_to_view"
    FIRST_TYPE = "TABLE"
    SECOND_TYPE = "VIEW"


class TestAViewThatBecomesATable(_TypeChangeCase):
    DATABASE_NAME = "type_change_view_to_table"
    FIRST_TYPE = "VIEW"
    SECOND_TYPE = "TABLE"
