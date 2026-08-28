"""The shared fixtures of the Clair tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import structlog


@pytest.fixture
def simple_project() -> Path:
    """The path of the simple_project test fixture."""
    return Path(__file__).parent / "fixtures" / "simple_project"


@pytest.fixture
def cyclic_project() -> Path:
    """The path of the cyclic_project test fixture."""
    return Path(__file__).parent / "fixtures" / "cyclic_project"


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
            for part in ("source.", "analytics.", "db.", "tmp_project", "_clair_routing_")
        ):
            del sys.modules[mod_name]


@pytest.fixture
def tmp_environments(tmp_path: Path) -> Path:
    """Make a temporary environments.yml file for a test."""
    environments_content = """
dev:
  account: test-account
  user: test-user
  authenticator: externalbrowser
  warehouse: test_wh
  role: test_role

ci:
  account: test-account
  user: ci-user
  password: ci-password
  warehouse: ci_wh
  role: ci_role

key_auth:
  account: test-account
  user: key-user
  private_key_path: /secrets/snowflake_key.p8
  warehouse: key_wh

key_auth_encrypted:
  account: test-account
  user: key-user
  private_key_path: /secrets/snowflake_key_enc.p8
  private_key_passphrase: s3cr3t
  warehouse: key_wh

unknown_key:
  account: test-account
  user: test-user
  authenticator: externalbrowser
  warehouse: test_wh
  routing:
    policy: database_override
"""
    environments_file = tmp_path / "environments.yml"
    environments_file.write_text(environments_content)
    return environments_file


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
