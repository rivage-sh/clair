"""Fixtures for the integration tests.

Without the Snowflake settings the tests skip on a workstation, and they fail in
GitHub Actions. A run with no credentials in CI would otherwise report success
after it ran nothing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.clean_up import drop_schema
from tests.integration.config import (
    ENVIRONMENT_NAME,
    IntegrationConfig,
    IntegrationConfigError,
    load_config,
)
from tests.integration.setup import prepare
from tests.integration.warehouse import connect

COMMAND_TIMEOUT_SECONDS = 900


@pytest.fixture(scope="session")
def integration_config() -> IntegrationConfig:
    """Give the settings of the run, or stop the tests."""
    try:
        return load_config()
    except IntegrationConfigError as error:
        # The integration workflow sets this variable. The other CI jobs run the
        # same files with no credentials, and those must skip, not fail.
        if os.environ.get("CLAIR_CI_REQUIRE_SNOWFLAKE"):
            pytest.fail(f"The integration job needs the Snowflake settings: {error}")
        pytest.skip(str(error))


@pytest.fixture(scope="session")
def snowflake_workspace(integration_config: IntegrationConfig) -> Iterator[IntegrationConfig]:
    """Make the schema of the run, and drop it after the last test."""
    prepare(integration_config)
    yield integration_config

    adapter = connect(integration_config)
    try:
        drop_schema(adapter, integration_config.schema_name)
    finally:
        adapter.close()


@pytest.fixture(scope="session")
def adapter(snowflake_workspace: IntegrationConfig) -> Iterator[SnowflakeAdapter]:
    """Give one open connection for the assertions."""
    connection = connect(snowflake_workspace)
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def clair_home(
    snowflake_workspace: IntegrationConfig, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Write an environments.yml for the run, in a private HOME."""
    home = tmp_path_factory.mktemp("clair_home")
    clair_dir = home / ".clair"
    clair_dir.mkdir()
    environments_file = clair_dir / "environments.yml"
    environments_file.write_text(snowflake_workspace.to_environments_yaml())
    environments_file.chmod(0o600)
    return home


def clair_environment(config: IntegrationConfig, home: Path) -> dict[str, str]:
    """Give the environment variables for one clair command."""
    environment = dict(os.environ)
    environment["HOME"] = str(home)
    environment["USERPROFILE"] = str(home)
    environment["CLAIR_ENV"] = ENVIRONMENT_NAME
    environment["CLAIR_LOG_FORMAT"] = "json"
    environment["CLAIR_CI_SCHEMA_NAME"] = config.schema_name
    environment["CLAIR_CI_SNOWFLAKE_WAREHOUSE"] = config.warehouse
    environment["CLAIR_CI_SNOWFLAKE_ROLE"] = config.role
    return environment


def run_clair(
    arguments: list[str], environment: dict[str, str], expect_success: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run the clair CLI as a subprocess."""
    completed = subprocess.run(
        [sys.executable, "-m", "clair.cli.main", *arguments],
        capture_output=True,
        text=True,
        env=environment,
        timeout=COMMAND_TIMEOUT_SECONDS,
        check=False,
    )
    if expect_success and completed.returncode != 0:
        raise AssertionError(
            f"clair {' '.join(arguments)} gave {completed.returncode}.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def log_events(completed: subprocess.CompletedProcess[str]) -> list[dict[str, object]]:
    """Give each structured log event of one command.

    CLAIR_LOG_FORMAT=json makes one JSON object for each line. A test reads the
    fields, thus a change of the human text breaks no test.
    """
    events: list[dict[str, object]] = []
    for line in completed.stdout.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            events.append(json.loads(stripped))
        except json.JSONDecodeError:
            continue
    return events


def events_named(
    completed: subprocess.CompletedProcess[str], event_name: str
) -> list[dict[str, object]]:
    """Give each event with one name."""
    return [event for event in log_events(completed) if event.get("event") == event_name]
