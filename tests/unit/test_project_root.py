"""The tests of the project root: the root search, the boundary, module identity."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

import clair
from clair.cli.main import cli
from clair.core.dag import build_dag
from clair.core.discovery import discover_project, find_project_root
from clair.exceptions import (
    NotAProjectRootError,
    ProjectDiscoveryError,
    ProjectRootNotFoundError,
)
from clair.trouves._refs import TROUVE_PLACEHOLDER_PREFIX
from clair.trouves.trouve import CompiledAttributes, TrouveAbc
from tests.helpers import write_project_marker


def compiled_of(trouves: list[TrouveAbc], logical_address: str) -> CompiledAttributes:
    """Give the compiled attributes of one Trouve, by its logical address."""
    for trouve in trouves:
        assert trouve.compiled is not None
        if str(trouve.compiled.logical_address) == logical_address:
            return trouve.compiled
    raise AssertionError(f"the project holds no Trouve at {logical_address}")

SOURCE_FILE = """\
from clair import Trouve, TrouveType

trouve = Trouve(type=TrouveType.SOURCE)
"""


def write_flat_project(project_root: Path) -> None:
    """Write a project with one SOURCE and one Trouve that reads it.

    The refined Trouve imports the source from the project root, which is the
    import style of the documentation.
    """
    write_project_marker(project_root)
    (project_root / "shop" / "source").mkdir(parents=True, exist_ok=True)
    (project_root / "shop" / "refined").mkdir(parents=True, exist_ok=True)
    (project_root / "shop" / "source" / "orders.py").write_text(SOURCE_FILE)
    (project_root / "shop" / "refined" / "daily.py").write_text(
        "from shop.source.orders import trouve as orders\n"
        "from clair import Trouve\n"
        'trouve = Trouve(sql=f"SELECT * FROM {orders}")\n'
    )


class TestTheRootSearch:
    """find_project_root walks up to the first __routing__.py."""

    def test_it_finds_the_marker_in_the_directory(self, tmp_path: Path) -> None:
        assert find_project_root(tmp_path) == tmp_path

    def test_it_finds_the_marker_from_a_subdirectory(self, tmp_path: Path) -> None:
        subdirectory = tmp_path / "shop" / "refined"
        subdirectory.mkdir(parents=True)
        assert find_project_root(subdirectory) == tmp_path

    def test_it_takes_the_nearest_root_of_two(self, tmp_path: Path) -> None:
        inner_root = write_project_marker(tmp_path / "inner")
        subdirectory = inner_root / "shop"
        subdirectory.mkdir()
        assert find_project_root(subdirectory) == inner_root

    def test_no_marker_above_gives_an_error(self, tmp_path: Path) -> None:
        (tmp_path / "__routing__.py").unlink()
        with pytest.raises(ProjectRootNotFoundError) as error:
            find_project_root(tmp_path)
        assert "__routing__.py" in str(error.value)


class TestTheBoundary:
    """A directory that holds many projects is not a project root."""

    def test_a_directory_of_projects_raises(self, tmp_path: Path) -> None:
        (tmp_path / "__routing__.py").unlink()
        write_flat_project(tmp_path / "project_a")
        write_flat_project(tmp_path / "project_b")
        with pytest.raises(NotAProjectRootError) as error:
            discover_project(tmp_path)
        assert str(tmp_path) in str(error.value)

    def test_one_project_below_the_container_still_works(self, tmp_path: Path) -> None:
        (tmp_path / "__routing__.py").unlink()
        write_flat_project(tmp_path / "project_a")
        trouves = discover_project(tmp_path / "project_a")
        assert len(trouves) == 2


class TestOneModulePerFile:
    """One file gives one module object, under every import name.

    The project sits inside a package of a monorepo. Discovery imports a file
    from the project root, and the author imports the same file through the
    package. Without the module identity finder, the file runs two times, the
    second Trouve object is unknown to clair, and a reference token stays in
    the SQL.
    """

    @pytest.fixture()
    def monorepo_project(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        """A project at monorepo/clair_projects/analytics, with monorepo on sys.path.

        The author's import resolves through the monorepo root, in the same way
        as in a notebook whose working directory is the monorepo.
        """
        monorepo = tmp_path / "monorepo"
        project_root = monorepo / "clair_projects" / "analytics"
        write_project_marker(project_root)
        (project_root / "shop" / "source").mkdir(parents=True)
        (project_root / "shop" / "refined").mkdir(parents=True)
        (project_root / "shop" / "source" / "orders.py").write_text(SOURCE_FILE)
        (project_root / "shop" / "refined" / "daily.py").write_text(
            "from clair_projects.analytics.shop.source.orders import trouve as orders\n"
            "from clair import Trouve\n"
            'trouve = Trouve(sql=f"SELECT * FROM {orders}")\n'
        )
        monkeypatch.syspath_prepend(str(monorepo))
        return project_root

    def test_the_sql_keeps_no_reference_token(self, monorepo_project: Path) -> None:
        trouves = discover_project(monorepo_project)
        refined = compiled_of(trouves, "shop.refined.daily")
        assert TROUVE_PLACEHOLDER_PREFIX not in refined.resolved_sql
        assert "shop.source.orders" in refined.resolved_sql

    def test_the_dag_keeps_the_edge(self, monorepo_project: Path) -> None:
        trouves = discover_project(monorepo_project)
        dag = build_dag(trouves)
        refined = compiled_of(trouves, "shop.refined.daily")
        assert refined.imports == ["shop.source.orders"]
        assert len(dag.edges) == 1

    def test_clair_loads_the_file_one_time(self, monorepo_project: Path) -> None:
        discover_project(monorepo_project)
        source_file = monorepo_project / "shop" / "source" / "orders.py"
        modules = [
            module
            for module in list(sys.modules.values())
            if (module_file := getattr(module, "__file__", None))
            and Path(module_file).resolve() == source_file.resolve()
        ]
        assert len(modules) >= 1
        assert len({id(module) for module in modules}) == 1

    def test_a_package_chain_needs_no_sys_path_entry(self, tmp_path: Path) -> None:
        """__init__.py files above the root give the same result, with no setup.

        No caller puts the monorepo on sys.path here. Discovery walks up the
        __init__.py chain and finds the import root itself.
        """
        monorepo = tmp_path / "monorepo"
        project_root = monorepo / "clair_projects" / "analytics"
        write_project_marker(project_root)
        (monorepo / "clair_projects" / "__init__.py").write_text("")
        (project_root / "__init__.py").write_text("")
        (project_root / "shop" / "source").mkdir(parents=True)
        (project_root / "shop" / "refined").mkdir(parents=True)
        (project_root / "shop" / "source" / "orders.py").write_text(SOURCE_FILE)
        (project_root / "shop" / "refined" / "daily.py").write_text(
            "from clair_projects.analytics.shop.source.orders import trouve as orders\n"
            "from clair import Trouve\n"
            'trouve = Trouve(sql=f"SELECT * FROM {orders}")\n'
        )

        trouves = discover_project(project_root)

        refined = compiled_of(trouves, "shop.refined.daily")
        assert TROUVE_PLACEHOLDER_PREFIX not in refined.resolved_sql
        assert refined.imports == ["shop.source.orders"]


    def test_the_author_import_takes_the_module_that_discovery_loaded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The upstream file loads first, thus the author import must take it.

        Discovery reads the candidates in path order. ``a_source`` sorts before
        ``b_derived``, thus clair imports the source file under its own name
        first. The import of the author then names the same file differently,
        and the finder must give it the module that already exists.
        """
        monorepo = tmp_path / "monorepo"
        project_root = monorepo / "clair_projects" / "analytics"
        write_project_marker(project_root)
        (project_root / "shop" / "a_source").mkdir(parents=True)
        (project_root / "shop" / "b_derived").mkdir(parents=True)
        (project_root / "shop" / "a_source" / "orders.py").write_text(SOURCE_FILE)
        (project_root / "shop" / "b_derived" / "daily.py").write_text(
            "from clair_projects.analytics.shop.a_source.orders import trouve as orders\n"
            "from clair import Trouve\n"
            'trouve = Trouve(sql=f"SELECT * FROM {orders}")\n'
        )
        monkeypatch.syspath_prepend(str(monorepo))

        trouves = discover_project(project_root)

        derived = compiled_of(trouves, "shop.b_derived.daily")
        assert TROUVE_PLACEHOLDER_PREFIX not in derived.resolved_sql
        assert derived.imports == ["shop.a_source.orders"]

        source_file = project_root / "shop" / "a_source" / "orders.py"
        objects = {
            id(module)
            for module in list(sys.modules.values())
            if (module_file := getattr(module, "__file__", None))
            and Path(module_file).resolve() == source_file.resolve()
        }
        assert len(objects) == 1


class TestTheFlatProject:
    """A project outside every package keeps its behaviour."""

    def test_a_flat_project_resolves_its_references(self, tmp_path: Path) -> None:
        write_flat_project(tmp_path)
        trouves = discover_project(tmp_path)
        refined = compiled_of(trouves, "shop.refined.daily")
        assert refined.resolved_sql == "SELECT * FROM shop.source.orders"
        assert refined.imports == ["shop.source.orders"]


class TestTheTokenGuard:
    """A reference token that survives compilation stops the run."""

    def test_a_trouve_from_outside_the_project_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "foreign_trouve_module.py").write_text(SOURCE_FILE)
        monkeypatch.syspath_prepend(str(outside))

        project_root = write_project_marker(tmp_path / "project")
        (project_root / "shop" / "refined").mkdir(parents=True)
        (project_root / "shop" / "refined" / "daily.py").write_text(
            "from foreign_trouve_module import trouve as foreign\n"
            "from clair import Trouve\n"
            'trouve = Trouve(sql=f"SELECT * FROM {foreign}")\n'
        )

        with pytest.raises(ProjectDiscoveryError) as error:
            discover_project(project_root)
        assert "reference token" in str(error.value)
        sys.modules.pop("foreign_trouve_module", None)


class TestTheCommands:
    """The CLI finds the root from the working directory."""

    def test_a_command_runs_from_a_subdirectory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_flat_project(tmp_path)
        monkeypatch.chdir(tmp_path / "shop" / "refined")
        result = CliRunner().invoke(cli, ["dag"])
        assert result.exit_code == 0, result.output
        assert "shop.refined.daily" in result.output

    def test_a_directory_outside_a_project_names_the_marker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "__routing__.py").unlink()
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli, ["dag"])
        assert result.exit_code == 1
        assert "__routing__.py" in result.output

    def test_init_refuses_inside_a_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        subdirectory = tmp_path / "shop"
        subdirectory.mkdir()
        result = CliRunner().invoke(cli, ["init", "--project", str(subdirectory)])
        assert result.exit_code == 1
        assert "__routing__.py" in result.output


class TestThePythonApi:
    """clair.validate(None) starts the root search from the working directory."""

    def test_none_starts_the_root_search(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_flat_project(tmp_path)
        monkeypatch.chdir(tmp_path / "shop")
        report = clair.validate()
        assert report.is_valid


class TestTheConfigOfTwoProjects:
    """Two projects of one monorepo keep their own __database_config__.py."""

    def test_each_project_reads_its_own_config(self, tmp_path: Path) -> None:
        for project_name, warehouse_name in (("alpha", "wh_alpha"), ("beta", "wh_beta")):
            project_root = write_project_marker(tmp_path / project_name)
            (project_root / "shop" / "source").mkdir(parents=True)
            (project_root / "shop" / "source" / "orders.py").write_text(SOURCE_FILE)
            (project_root / "shop" / "__database_config__.py").write_text(
                "from clair import DatabaseDefaults\n"
                f'defaults = DatabaseDefaults(warehouse="{warehouse_name}")\n'
            )

        alpha_trouves = discover_project(tmp_path / "alpha")
        beta_trouves = discover_project(tmp_path / "beta")

        assert compiled_of(alpha_trouves, "shop.source.orders").config.warehouse == (
            "wh_alpha"
        )
        assert compiled_of(beta_trouves, "shop.source.orders").config.warehouse == (
            "wh_beta"
        )
