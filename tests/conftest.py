"""Shared test fixtures for Clair tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture
def simple_project() -> Path:
    """Path to the simple_project test fixture."""
    return Path(__file__).parent / "fixtures" / "simple_project"


@pytest.fixture
def cyclic_project() -> Path:
    """Path to the cyclic_project test fixture."""
    return Path(__file__).parent / "fixtures" / "cyclic_project"


@pytest.fixture(autouse=True)
def clean_sys_modules():
    """Remove any fixture-loaded modules from sys.modules between tests.

    Prevents state leakage between tests that load Trouve files.
    """
    before = set(sys.modules.keys())
    yield
    after = set(sys.modules.keys())
    for mod_name in after - before:
        if any(
            part in mod_name
            for part in ("source.", "analytics.", "db.", "tmp_project")
        ):
            del sys.modules[mod_name]


@pytest.fixture
def tmp_environments(tmp_path: Path) -> Path:
    """Create a temporary environments.yml for testing."""
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

with_routing:
  account: test-account
  user: test-user
  authenticator: externalbrowser
  warehouse: test_wh
  routing:
    policy: database_override
    database_name: OMER_DEV

with_schema_isolation:
  account: test-account
  user: test-user
  authenticator: externalbrowser
  warehouse: test_wh
  routing:
    policy: schema_isolation
    database_name: DEV
    schema_name: obaddour
"""
    environments_file = tmp_path / "environments.yml"
    environments_file.write_text(environments_content)
    return environments_file
