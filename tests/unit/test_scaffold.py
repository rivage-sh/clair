"""The tests of clair.core.scaffold. That module starts a new project."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.core.discovery import discover_project
from clair.core.scaffold import add_artifacts_to_gitignore, scaffold_project, write_environments_yml
from clair.environments.project_routing import load_project_routing
from clair.environments.routing import route
from clair.trouves.trouve_type import TrouveType
from tests.helpers import make_environment

# ---------------------------------------------------------------------------
# The helper functions.
# ---------------------------------------------------------------------------

EXPECTED_PROJECT_FILES = [
    "source/raw/orders.py",
]

DEFAULT_SOURCE_ARGS = {
    "source_database_name": "source",
    "source_schema_name": "raw",
    "source_table_name": "orders",
    "env_name": "dev",
}


def _run_scaffold(tmp_path: Path) -> list[tuple[str, str]]:
    """Run scaffold_project in a temporary directory."""
    return scaffold_project(tmp_path / "my_project", **DEFAULT_SOURCE_ARGS)


# ---------------------------------------------------------------------------
# The tests.
# ---------------------------------------------------------------------------


class TestScaffoldCreatesAllExpectedFiles:
    def test_creates_all_project_files(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "my_project"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        for relative_path in EXPECTED_PROJECT_FILES:
            full_path = project_dir / relative_path
            assert full_path.exists(), f"Missing: {relative_path}"

    def test_creates_routing_file(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "my_project"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        assert (project_dir / "__routing__.py").exists()

    def test_returns_all_paths_as_created(self, tmp_path: Path) -> None:
        results = _run_scaffold(tmp_path)

        # 1 source Trouve and 1 __routing__.py file.
        assert len(results) == 2
        assert all(status == "created" for status, _ in results)


class TestFileContents:
    def test_orders_trouve_has_source_type(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        (orders,) = discover_project(project_dir)
        assert orders.type == TrouveType.SOURCE

    def test_routing_file_gives_a_passthrough_entry_for_dev(self, tmp_path: Path) -> None:
        """The file that clair init writes loads, and it changes no address."""
        project_dir = tmp_path / "proj"
        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        project_routing = load_project_routing(project_dir, "dev")

        assert project_routing.entry is not None
        physical_address = route(
            "analytics.orders.daily", TrouveType.TABLE, project_routing.entry
        )
        assert str(physical_address) == "analytics.orders.daily"

    @pytest.mark.parametrize("env_name", ["local", "omer-dev", "2026_sandbox"])
    def test_routing_file_names_the_environment_that_it_gets(
        self, tmp_path: Path, env_name: str
    ) -> None:
        """A name other than dev gives an entry for that name, and no entry for dev."""
        project_dir = tmp_path / "proj"
        source_args = {**DEFAULT_SOURCE_ARGS, "env_name": env_name}
        scaffold_project(project_dir, **source_args)

        assert load_project_routing(project_dir, env_name).entry is not None
        assert load_project_routing(project_dir, "dev").entry is None


class TestDoesNotOverwriteExistingFiles:
    def test_skips_existing_project_file(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        orders_path = project_dir / "source" / "raw" / "orders.py"
        orders_path.parent.mkdir(parents=True)
        orders_path.write_text("# my custom file\n")

        results = scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        assert orders_path.read_text() == "# my custom file\n"

        results_dict = {path: status for status, path in results}
        assert results_dict[str(orders_path)] == "skipped"

class TestGitignore:
    """`clair init` adds the artifacts directory to .gitignore, and keeps each other line."""

    @pytest.mark.parametrize(
        ("existing_text", "expected_text", "expected_status"),
        [
            pytest.param(None, "/_clairtifacts\n", "created", id="no_file"),
            pytest.param("venv/\n", "venv/\n/_clairtifacts\n", "updated", id="a_file_of_the_repository"),
            pytest.param("venv/", "venv/\n/_clairtifacts\n", "updated", id="no_final_line_break"),
            pytest.param("/_clairtifacts\nvenv/\n", "/_clairtifacts\nvenv/\n", "skipped", id="the_line_exists"),
        ],
    )
    def test_the_artifacts_line_joins_the_file(
        self, tmp_path: Path, existing_text, expected_text, expected_status
    ) -> None:
        gitignore_path = tmp_path / ".gitignore"
        if existing_text is not None:
            gitignore_path.write_text(existing_text)

        status, path = add_artifacts_to_gitignore(tmp_path)

        assert (status, path) == (expected_status, gitignore_path)
        assert gitignore_path.read_text() == expected_text


class TestProjectDirCreation:
    def test_creates_project_dir_if_missing(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "brand_new" / "nested" / "project"
        assert not project_dir.exists()

        scaffold_project(project_dir, **DEFAULT_SOURCE_ARGS)

        assert project_dir.exists()
        assert (project_dir / "source" / "raw" / "orders.py").exists()


class TestCustomSourceDbName:
    def test_source_directory_named_after_source_db(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "proj"
        scaffold_project(
            project_dir,
            source_database_name="mydb",
            source_schema_name="myschema",
            source_table_name="mytable",
            env_name="dev",
        )

        assert (project_dir / "mydb" / "myschema" / "mytable.py").exists()


class TestWriteEnvironmentsYml:
    """test_environments.py proves that the loader reads back what the writer writes."""

    def test_the_file_goes_to_the_clair_directory_of_the_home(self, tmp_path: Path) -> None:
        path = write_environments_yml(make_environment(), home_dir=tmp_path)
        assert path == tmp_path / ".clair" / "environments.yml"

    def test_only_the_owner_reads_the_file(self, tmp_path: Path) -> None:
        # The file can hold a password, thus no other user reads it.
        path = write_environments_yml(make_environment(), home_dir=tmp_path)
        assert path.stat().st_mode & 0o777 == 0o600
