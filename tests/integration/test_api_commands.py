"""The Python API commands against Snowflake: exclude, threads, compile, clean.

`tests/integration/test_examples.py` covers a full refresh, the incremental
modes and `select`. This file covers the commands beside them, with the same
example projects and the same routing entry.

Each test calls the function that `clair.<command>` gives, and it reads the
result object. A user of the Python API reads the same object.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    copy_with_ci_routing,
    example_project_paths,
    model_logical_names,
    physical_address,
    trouves_of,
)

pytestmark = pytest.mark.integration

# One project is enough for these commands. example_1 is the smallest project
# that holds a SOURCE Trouve and more than one model.
PROJECT_NAME = "example_1"
EVENTS_LOGICAL_NAME = "example_1_database.refined.events"


@pytest.fixture(scope="module")
def project_copy(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Copy the project once, with the test routing entry."""
    destination = tmp_path_factory.mktemp("api_commands")
    project_path = next(
        path for path in example_project_paths() if path.name == PROJECT_NAME
    )
    return copy_with_ci_routing(project_path, destination)


@pytest.fixture(scope="module")
def project_source_path() -> Path:
    """Give the project in the repository, which holds the logical names."""
    return next(
        path for path in example_project_paths() if path.name == PROJECT_NAME
    )


class TestExclude:
    """`exclude` removes a Trouve after the selection."""

    def test_exclude_removes_the_named_trouve(
        self,
        project_copy: Path,
        project_source_path: Path,
        clair_environment: IntegrationConfig,
    ) -> None:
        excluded = str(
            physical_address(EVENTS_LOGICAL_NAME, clair_environment.schema_name)
        )

        summary = clair.run(project_copy, exclude=[excluded])

        built = [result.logical_address for result in summary.succeeded]
        assert EVENTS_LOGICAL_NAME not in built
        assert summary.failed == []

    def test_exclude_keeps_each_other_trouve(
        self,
        project_copy: Path,
        project_source_path: Path,
        clair_environment: IntegrationConfig,
    ) -> None:
        excluded = str(
            physical_address(EVENTS_LOGICAL_NAME, clair_environment.schema_name)
        )
        model_count = len(model_logical_names(trouves_of(project_source_path)))

        summary = clair.run(project_copy, exclude=[excluded])

        assert summary.succeeded_count == model_count - 1


class TestThreads:
    """A parallel run gives the same result as a run with one thread."""

    def test_a_parallel_run_builds_each_trouve(
        self,
        project_copy: Path,
        project_source_path: Path,
        clair_environment: IntegrationConfig,
    ) -> None:
        model_count = len(model_logical_names(trouves_of(project_source_path)))

        summary = clair.run(project_copy, threads=4)

        assert summary.failed == []
        assert summary.succeeded_count == model_count

    def test_a_parallel_run_builds_the_same_trouves_as_one_thread(
        self, project_copy: Path, clair_environment: IntegrationConfig
    ) -> None:
        one_thread = clair.run(project_copy, threads=1)
        four_threads = clair.run(project_copy, threads=4)

        assert sorted(result.logical_address for result in one_thread.succeeded) == (
            sorted(result.logical_address for result in four_threads.succeeded)
        )


class TestCompile:
    """`clair compile` writes the SQL, and it opens no connection."""

    def test_compile_writes_one_artifact_for_each_trouve(
        self,
        project_copy: Path,
        project_source_path: Path,
        clair_environment: IntegrationConfig,
    ) -> None:
        model_count = len(model_logical_names(trouves_of(project_source_path)))

        output = clair.compile(project_copy)

        assert len(output.compiled_nodes) == model_count
        for node in output.compiled_nodes:
            assert node.artifact_path is not None
            assert node.artifact_path.exists()

    def test_compile_routes_each_address_to_the_schema_of_the_run(
        self, project_copy: Path, clair_environment: IntegrationConfig
    ) -> None:
        output = clair.compile(project_copy)
        expected = str(
            physical_address(EVENTS_LOGICAL_NAME, clair_environment.schema_name)
        )
        assert any(node.physical_address == expected for node in output.compiled_nodes)


class TestClean:
    """`clair clean` removes the artifacts that the runs wrote."""

    def test_clean_removes_each_run_of_a_project(
        self, tmp_path_factory: pytest.TempPathFactory, clair_environment
    ) -> None:
        """A compile writes one run directory, and clean removes it."""
        destination = tmp_path_factory.mktemp("clean_project")
        project_path = next(
            path for path in example_project_paths() if path.name == PROJECT_NAME
        )
        copy_path = copy_with_ci_routing(project_path, destination)

        clair.compile(copy_path, run_mode=RunMode.FULL_REFRESH)
        plan = clair.clean(copy_path, dry_run=True)
        assert plan.run_count >= 1

        output = clair.clean(copy_path)
        assert output.run_count == plan.run_count
        for run in output.runs:
            assert not run.path.exists()
