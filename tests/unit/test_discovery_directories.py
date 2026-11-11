"""Which directories discovery reads.

No name is special: a database or a schema can be called ``tests`` or
``clair``. Discovery never imports a virtual environment, whatever its name,
a directory that starts with ``.``, or a helper directory that starts with
``_``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from tests.helpers import write_project_marker

TROUVE_FILE = 'from clair import Table\ntrouve = Table(sql="select 1 as id")\n'
EXPLODING_FILE = 'raise RuntimeError("discovery imported a file that it must skip")\n'


def _logical_addresses(project_root: Path) -> list[str]:
    output = clair.compile(project_root, env="dev")
    return sorted(str(node.addresses.logical) for node in output.compiled_nodes)


@pytest.mark.parametrize(
    "relative_path",
    [
        "analytics/tests/orders.py",
        "tests/core/orders.py",
        "clair/core/orders.py",
        "analytics/clair/orders.py",
        "node_modules/core/orders.py",
    ],
)
def test_no_directory_name_is_special(tmp_path, relative_path):
    project_root = write_project_marker(tmp_path / "project")
    trouve_path = project_root / relative_path
    trouve_path.parent.mkdir(parents=True)
    trouve_path.write_text(TROUVE_FILE)

    assert _logical_addresses(project_root) == [relative_path.removesuffix(".py").replace("/", ".")]


@pytest.mark.parametrize(
    "skipped_directory",
    [
        "venv.nosync/lib/python3.14/site-packages/somepackage",
        ".venv/lib/somepackage",
        ".git/hooks/core",
        "env/lib/python3.14/site-packages/somepackage",
        "analytics/_helpers",
        "_clairtifacts/run/core",
        "__pycache__/core/stale",
        "analytics/core/__pycache__",
    ],
)
def test_a_skipped_directory_is_never_imported(tmp_path, skipped_directory):
    project_root = write_project_marker(tmp_path / "project")
    (project_root / "analytics" / "core").mkdir(parents=True)
    (project_root / "analytics" / "core" / "orders.py").write_text(TROUVE_FILE)
    (project_root / skipped_directory).mkdir(parents=True, exist_ok=True)
    (project_root / skipped_directory / "module.py").write_text(EXPLODING_FILE)
    # A virtual environment holds pyvenv.cfg at its top, whatever its name.
    for venv_name in ("venv.nosync", "env", ".venv"):
        if skipped_directory.startswith(venv_name + "/"):
            (project_root / venv_name / "pyvenv.cfg").write_text("home = /usr/bin\n")

    assert _logical_addresses(project_root) == ["analytics.core.orders"]


def test_a_database_called_clair_does_not_collide_with_the_clair_package(tmp_path):
    # clair.core.dag is also a module of clair itself. The project file must
    # not replace it, and a Trouve of the project imports another one there.
    project_root = write_project_marker(tmp_path / "project")
    (project_root / "clair" / "core").mkdir(parents=True)
    (project_root / "clair" / "core" / "dag.py").write_text(TROUVE_FILE)
    (project_root / "clair" / "core" / "summary.py").write_text(
        "from clair.core.dag import trouve as dag\n"
        "from clair import Table\n"
        'trouve = Table(sql=f"select * from {dag}")\n'
    )

    output = clair.compile(project_root, env="dev")

    (summary,) = [node for node in output.compiled_nodes if str(node.addresses.logical) == "clair.core.summary"]
    assert summary.dependencies == ["clair.core.dag"]


def test_a_helper_directory_that_a_trouve_imports_still_works(tmp_path):
    # Discovery skips _helpers, but Python imports it for the Trouve.
    project_root = write_project_marker(tmp_path / "project")
    (project_root / "analytics" / "_helpers").mkdir(parents=True)
    (project_root / "analytics" / "_helpers" / "__init__.py").write_text("")
    (project_root / "analytics" / "_helpers" / "sql.py").write_text('ONE = "select 1 as id"\n')
    (project_root / "analytics" / "core").mkdir(parents=True)
    (project_root / "analytics" / "core" / "orders.py").write_text(
        "from analytics._helpers.sql import ONE\n"
        "from clair import Table\n"
        "trouve = Table(sql=ONE)\n"
    )

    assert _logical_addresses(project_root) == ["analytics.core.orders"]
