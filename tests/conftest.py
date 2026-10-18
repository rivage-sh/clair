"""The shared fixtures of the Clair tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import structlog

from tests.helpers import write_project_marker


@pytest.fixture(autouse=True)
def project_marker(tmp_path: Path) -> Path:
    """Write ``__routing__.py`` in tmp_path, thus tmp_path is a project root.

    Discovery reads the marker, and almost every test that writes a project
    writes it in tmp_path. A test that needs a project root below tmp_path
    writes its own marker with ``write_project_marker``. A test that writes its
    own ``__routing__.py`` into tmp_path replaces this one.
    """
    return write_project_marker(tmp_path)


@pytest.fixture
def simple_project() -> Path:
    """The path of the simple_project test fixture."""
    return Path(__file__).parent / "fixtures" / "simple_project"


@pytest.fixture
def cyclic_project() -> Path:
    """The path of the cyclic_project test fixture."""
    return Path(__file__).parent / "fixtures" / "cyclic_project"


@pytest.fixture(autouse=True)
def no_home_environments_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the default environments.yml at a file that does not exist.

    A unit test must not read ~/.clair/environments.yml of the developer. A test
    that needs the file writes one, and gives its path.
    """
    monkeypatch.setattr(
        "clair.environments.environments.DEFAULT_ENVIRONMENTS_PATH",
        tmp_path / "no_home" / "environments.yml",
    )


@pytest.fixture(autouse=True)
def clean_sys_modules():
    """Delete each fixture module from sys.modules after each test.

    Thus no state moves from one test to the next test. This is important for
    the tests that load a Trouve file.
    """
    before = set(sys.modules.keys())
    yield
    after = set(sys.modules.keys())
    for mod_name in after - before:
        if any(
            part in mod_name
            for part in (
                "source.",
                "analytics.",
                "db.",
                "tmp_project",
                "_clair_support_",
                "clair_projects",
            )
        ):
            del sys.modules[mod_name]


@pytest.fixture
def routing_project(tmp_path: Path) -> Path:
    """Create a project directory that holds a __routing__.py file."""
    project_dir = tmp_path / "routing_project"
    project_dir.mkdir()
    return project_dir


@pytest.fixture(autouse=True)
def reset_structlog():
    """Give each test the default structlog configuration.

    `configure_logging` binds the logger to `sys.stderr` at the time of the
    call. A test that runs the CLI with CliRunner leaves the logger bound to the
    stream that CliRunner then closes, and each later test fails with "I/O
    operation on closed file". The reset keeps the tests independent of order.
    """
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()
