"""The tests of core/project_imports.py: the module owns the import system.

The tests of the finder itself are in ``test_project_root.py``, because they
read a project through ``discover_project()``. The tests here state the
properties of the module: it holds each change to the import system, and it
gives one project a clean state.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from clair.core import project_imports
from clair.core.discovery import discover_project
from tests.helpers import write_project_marker

SOURCE_FILE = """\
from clair import Trouve, TrouveType

trouve = Trouve(type=TrouveType.SOURCE, sql="")
"""


def write_project(project_root: Path, marker: str) -> Path:
    """Write a project of two files. *marker* makes the SQL of each one unique."""
    write_project_marker(project_root)
    (project_root / "mydb" / "source").mkdir(parents=True)
    (project_root / "mydb" / "refined").mkdir(parents=True)
    (project_root / "mydb" / "source" / "rows.py").write_text(SOURCE_FILE)
    (project_root / "mydb" / "refined" / "checked.py").write_text(
        "from mydb.source.rows import trouve as rows\n"
        "from clair import Trouve\n"
        f"trouve = Trouve(sql=f\"select '{marker}' from {{rows}}\")\n"
    )
    return project_root


def sql_of(trouves, logical_address: str) -> str:
    for trouve in trouves:
        assert trouve.compiled is not None
        if str(trouve.compiled.logical_address) == logical_address:
            return trouve.compiled.resolved_sql
    raise AssertionError(f"the project holds no Trouve at {logical_address}")


class TestOneNameMeansOneThing:
    """Two projects of one process must not read the files of each other.

    A Trouve imports a different Trouve from the project root, thus the module
    name is meaningful inside one project only. sys.modules is global to the
    process, so the second project asks for a name that the first project
    holds.
    """

    def test_the_second_project_gives_its_own_sql(self, tmp_path: Path) -> None:
        discover_project(write_project(tmp_path / "first", "FIRST"))
        second = discover_project(write_project(tmp_path / "second", "SECOND"))
        assert "SECOND" in sql_of(second, "mydb.refined.checked")

    def test_the_first_project_leaves_sys_path(self, tmp_path: Path) -> None:
        first = write_project(tmp_path / "first", "FIRST")
        discover_project(first)
        discover_project(write_project(tmp_path / "second", "SECOND"))
        assert str(first) not in sys.path

    def test_one_project_two_times_reads_the_files_again(self, tmp_path: Path) -> None:
        """A notebook edits a Trouve file and runs the project again."""
        project_root = write_project(tmp_path / "only", "BEFORE")
        discover_project(project_root)
        (project_root / "mydb" / "refined" / "checked.py").write_text(
            "from mydb.source.rows import trouve as rows\n"
            "from clair import Trouve\n"
            'trouve = Trouve(sql=f"select \'AFTER\' from {rows}")\n'
        )
        trouves = discover_project(project_root)
        assert "AFTER" in sql_of(trouves, "mydb.refined.checked")


SOURCE_ROOT = Path(__file__).parent.parent.parent / "src" / "clair"

# The state of the import system that one module of clair owns.
IMPORT_SYSTEM_ATTRIBUTES = frozenset({"modules", "path", "meta_path"})

# core/project_imports.py owns each of them, and no other file of clair reads
# or writes one. Each loader of a clair file goes through that module.
PERMITTED_FILES = frozenset({Path("core/project_imports.py")})


def files_that_touch_the_import_system() -> list[str]:
    """Give each file of ``src/clair`` that reads or writes ``sys`` import state.

    The function reads the syntax tree, and not the text. A comment that names
    ``sys.modules`` is documentation, and it is not a use.
    """
    offenders: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        relative_path = path.relative_to(SOURCE_ROOT)
        if relative_path in PERMITTED_FILES:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            uses_attribute = (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "sys"
                and node.attr in IMPORT_SYSTEM_ATTRIBUTES
            )
            imports_the_name = (
                isinstance(node, ast.ImportFrom)
                and node.module == "sys"
                and any(alias.name in IMPORT_SYSTEM_ATTRIBUTES for alias in node.names)
            )
            if uses_attribute or imports_the_name:
                name = node.attr if isinstance(node, ast.Attribute) else "import"
                offenders.append(f"{relative_path}:{node.lineno} sys.{name}")
    return offenders


class TestTheModuleOwnsTheImportSystem:
    """One module changes sys.path, sys.modules, and sys.meta_path.

    The import system is global to the process, thus a second writer makes a
    fault that no reader of either file can see. The test states the boundary,
    because no type and no linter can.
    """

    def test_no_other_source_file_touches_the_import_system(self) -> None:
        assert files_that_touch_the_import_system() == []

    def test_the_test_finds_a_new_writer(self, tmp_path: Path) -> None:
        """The test above passes for a real reason, and not by accident."""
        offender = tmp_path / "offender.py"
        offender.write_text("import sys\n\nsys.modules.pop('x', None)\n")
        tree = ast.parse(offender.read_text())
        found = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "sys"
            and node.attr in IMPORT_SYSTEM_ATTRIBUTES
        ]
        assert len(found) == 1


class TestTheFinderStandsAside:
    """An import outside a project root goes through the normal machinery."""

    def test_a_stdlib_import_gives_the_normal_module(self, tmp_path: Path) -> None:
        discover_project(write_project(tmp_path / "only", "ONLY"))
        import json

        assert json.__name__ == "json"
        assert project_imports.module_for_file(Path(json.__file__)) is json

    def test_a_file_that_no_module_runs_gives_none(self, tmp_path: Path) -> None:
        assert project_imports.module_for_file(tmp_path / "absent.py") is None
