"""The tests of __clair_project__.py: the root search, the boundary, the anchor."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from clair.cli.main import cli
from clair.core.discovery import discover_project, find_project_root
from clair.exceptions import (
    InvalidProjectFileError,
    ProjectDiscoveryError,
    ProjectMarkerMissingError,
    ProjectRootNotFoundError,
)
from clair.trouves._refs import TROUVE_PLACEHOLDER_PREFIX
from clair.trouves.project_config import PROJECT_FILE_NAME, ProjectConfig
from clair.trouves.trouve import CompiledAttributes, TrouveAbc
from tests.helpers import write_project_marker


def compiled_of(trouve: TrouveAbc) -> CompiledAttributes:
    """Give the compiled attributes of *trouve*, and stop the test if absent."""
    assert trouve.compiled is not None
    return trouve.compiled


_SOURCE_TROUVE = """\
from clair import Trouve, TrouveType

trouve = Trouve(type=TrouveType.SOURCE)
"""


def _write_flat_project(project_root: Path) -> Path:
    """Write a project with one SOURCE Trouve, and give the root."""
    write_project_marker(project_root)
    table_file = project_root / "mydb" / "source" / "orders.py"
    table_file.parent.mkdir(parents=True, exist_ok=True)
    table_file.write_text(_SOURCE_TROUVE)
    return project_root


class TestTheRootSearch:
    """find_project_root walks up, in the same way that git finds .git."""

    def test_it_finds_the_marker_in_the_directory(self, tmp_path: Path) -> None:
        assert find_project_root(tmp_path) == tmp_path

    def test_it_finds_the_marker_from_a_subdirectory(self, tmp_path: Path) -> None:
        deep_directory = tmp_path / "mydb" / "source"
        deep_directory.mkdir(parents=True)

        assert find_project_root(deep_directory) == tmp_path

    def test_it_takes_the_nearest_root_of_two(self, tmp_path: Path) -> None:
        inner_root = write_project_marker(tmp_path / "inner")

        assert find_project_root(inner_root / "mydb") == inner_root

    def test_no_marker_above_gives_an_error(self, tmp_path: Path) -> None:
        (tmp_path / PROJECT_FILE_NAME).unlink()

        with pytest.raises(ProjectRootNotFoundError) as error:
            find_project_root(tmp_path)

        assert PROJECT_FILE_NAME in str(error.value)
        assert "clair init" in str(error.value)


class TestTheBoundary:
    """A directory that holds many projects is not one project."""

    def test_a_directory_of_projects_raises(self, tmp_path: Path) -> None:
        container = tmp_path / "projects"
        _write_flat_project(container / "project_one")
        _write_flat_project(container / "project_two")

        with pytest.raises(ProjectMarkerMissingError) as error:
            discover_project(container)

        assert PROJECT_FILE_NAME in str(error.value)

    def test_a_directory_of_projects_names_no_trouve(self, tmp_path: Path) -> None:
        """The error arrives before discovery reads one file."""
        container = tmp_path / "projects"
        _write_flat_project(container / "project_one")
        _write_flat_project(container / "project_two")

        with pytest.raises(ProjectMarkerMissingError) as error:
            discover_project(container)

        assert "orders" not in str(error.value)

    def test_one_project_below_the_container_still_works(self, tmp_path: Path) -> None:
        container = tmp_path / "projects"
        project_root = _write_flat_project(container / "project_one")

        trouves = discover_project(project_root)

        assert [str(compiled_of(trouve).logical_address) for trouve in trouves] == [
            "mydb.source.orders"
        ]


class TestTheProjectFile:
    """The marker file holds a ProjectConfig object."""

    def test_a_file_without_project_raises(self, tmp_path: Path) -> None:
        _write_flat_project(tmp_path)
        (tmp_path / PROJECT_FILE_NAME).write_text("value = 1\n")

        with pytest.raises(InvalidProjectFileError) as error:
            discover_project(tmp_path)

        assert "no `project` object" in str(error.value)

    def test_a_project_of_the_wrong_type_raises(self, tmp_path: Path) -> None:
        _write_flat_project(tmp_path)
        (tmp_path / PROJECT_FILE_NAME).write_text("project = 'analytics'\n")

        with pytest.raises(InvalidProjectFileError) as error:
            discover_project(tmp_path)

        assert "ProjectConfig" in str(error.value)

    def test_a_package_that_is_not_a_python_name_raises(self) -> None:
        with pytest.raises(ValueError, match="analytics-models"):
            ProjectConfig(package="clair_projects.analytics-models")

    def test_a_package_that_names_other_directories_raises(self, tmp_path: Path) -> None:
        _write_flat_project(tmp_path)
        (tmp_path / PROJECT_FILE_NAME).write_text(
            "from clair import ProjectConfig\n\n"
            "project = ProjectConfig(package='not_the.real_name')\n"
        )

        with pytest.raises(InvalidProjectFileError) as error:
            discover_project(tmp_path)

        assert "not_the.real_name" in str(error.value)


class TestTheImportRootOfAMonorepo:
    """The fault: one file, two module names, two Trouve objects, no DAG edge.

    A Trouve of a monorepo imports a different Trouve by the complete package
    name. Clair must load that file under the same name, because Python keys
    sys.modules by name. Two names give two module objects, thus two Trouve
    objects, and the reference token then stays in the SQL.
    """

    @staticmethod
    def _write_monorepo(tmp_path: Path) -> tuple[Path, Path]:
        """Give (the import root, the project root) of a monorepo project."""
        import_root = tmp_path / "monorepo"
        project_root = import_root / "clair_projects" / "analytics"
        write_project_marker(project_root)

        raw_file = project_root / "source" / "orders" / "raw.py"
        raw_file.parent.mkdir(parents=True)
        raw_file.write_text(_SOURCE_TROUVE)

        daily_file = project_root / "refined" / "orders" / "daily.py"
        daily_file.parent.mkdir(parents=True)
        daily_file.write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            from clair_projects.analytics.source.orders.raw import trouve as raw

            trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {raw}")
        """))
        return import_root, project_root

    def test_the_sql_keeps_no_reference_token(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The one assertion that catches the complete class of this fault."""
        import_root, project_root = self._write_monorepo(tmp_path)
        monkeypatch.syspath_prepend(str(import_root))

        trouves = discover_project(project_root)

        for trouve in trouves:
            assert TROUVE_PLACEHOLDER_PREFIX not in compiled_of(trouve).resolved_sql

    def test_the_dag_keeps_the_edge(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import_root, project_root = self._write_monorepo(tmp_path)
        monkeypatch.syspath_prepend(str(import_root))

        trouves = discover_project(project_root)
        imports_of = {
            str(compiled_of(trouve).logical_address): compiled_of(trouve).imports
            for trouve in trouves
        }

        assert imports_of["refined.orders.daily"] == ["source.orders.raw"]

    def test_the_sql_names_the_upstream_address(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import_root, project_root = self._write_monorepo(tmp_path)
        monkeypatch.syspath_prepend(str(import_root))

        trouves = discover_project(project_root)
        daily = next(
            trouve
            for trouve in trouves
            if str(compiled_of(trouve).logical_address) == "refined.orders.daily"
        )

        assert "source.orders.raw" in compiled_of(daily).resolved_sql

    def test_clair_loads_the_file_one_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The module name that clair makes is the name that the author writes."""
        import_root, project_root = self._write_monorepo(tmp_path)
        monkeypatch.syspath_prepend(str(import_root))

        discover_project(project_root)
        raw_file = project_root / "source" / "orders" / "raw.py"
        names = []
        for module_name, module in list(sys.modules.items()):
            module_file = getattr(module, "__file__", None)
            if module_file and Path(module_file).resolve() == raw_file.resolve():
                names.append(module_name)

        assert names == ["clair_projects.analytics.source.orders.raw"]

    def test_package_gives_the_same_result_as_the_sys_path_search(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`package` is the override for an import root that sys.path omits."""
        import_root, project_root = self._write_monorepo(tmp_path)
        (project_root / PROJECT_FILE_NAME).write_text(
            "from clair import ProjectConfig\n\n"
            "project = ProjectConfig(package='clair_projects.analytics')\n"
        )
        monkeypatch.syspath_prepend(str(import_root))

        trouves = discover_project(project_root)
        imports_of = {
            str(compiled_of(trouve).logical_address): compiled_of(trouve).imports
            for trouve in trouves
        }

        assert imports_of["refined.orders.daily"] == ["source.orders.raw"]


class TestTheFlatProject:
    """A project outside every package keeps the behaviour that it had."""

    def test_a_flat_project_resolves_its_references(self, tmp_path: Path) -> None:
        _write_flat_project(tmp_path)
        daily_file = tmp_path / "mydb" / "refined" / "daily.py"
        daily_file.parent.mkdir(parents=True)
        daily_file.write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType
            from mydb.source.orders import trouve as orders

            trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {orders}")
        """))

        trouves = discover_project(tmp_path)
        daily = next(
            trouve
            for trouve in trouves
            if str(compiled_of(trouve).logical_address) == "mydb.refined.daily"
        )

        assert compiled_of(daily).imports == ["mydb.source.orders"]
        assert TROUVE_PLACEHOLDER_PREFIX not in compiled_of(daily).resolved_sql


class TestTheTokenGuard:
    """Clair never sends SQL that keeps a reference token."""

    def test_a_trouve_from_outside_the_project_raises(self, tmp_path: Path) -> None:
        """An object that discovery never collects leaves its token behind."""
        _write_flat_project(tmp_path)
        daily_file = tmp_path / "mydb" / "refined" / "daily.py"
        daily_file.parent.mkdir(parents=True)
        daily_file.write_text(textwrap.dedent("""\
            from clair import Trouve, TrouveType

            outside = Trouve(type=TrouveType.SOURCE)
            trouve = Trouve(type=TrouveType.TABLE, sql=f"select * from {outside}")
        """))

        with pytest.raises(ProjectDiscoveryError) as error:
            discover_project(tmp_path)

        assert "mydb.refined.daily" in str(error.value)
        assert "token" in str(error.value)


class TestTheCommands:
    """The CLI finds the root, thus --project is an override."""

    def test_a_command_runs_from_a_subdirectory(self, tmp_path: Path) -> None:
        _write_flat_project(tmp_path)
        runner = CliRunner()

        with runner.isolated_filesystem():
            result = runner.invoke(
                cli, ["dag"], catch_exceptions=False, env={"PWD": str(tmp_path)}
            )

        # The isolated filesystem holds no marker, thus the command must say so.
        assert result.exit_code != 0
        assert PROJECT_FILE_NAME in result.output

    def test_the_dag_command_finds_the_root_above_the_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_flat_project(tmp_path)
        monkeypatch.chdir(tmp_path / "mydb" / "source")
        runner = CliRunner()

        result = runner.invoke(cli, ["dag"], catch_exceptions=False)

        assert result.exit_code == 0
        assert "mydb.source.orders" in result.output

    def test_init_refuses_inside_a_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_flat_project(tmp_path)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
        runner = CliRunner()

        result = runner.invoke(cli, ["init", "--project", str(tmp_path / "inside")])

        assert result.exit_code == 1
        assert PROJECT_FILE_NAME in result.output
