"""The fixtures of the integration tests.

Each test starts the clair CLI as a subprocess. A subprocess proves that the
installed entry point works, and it gives clair a HOME directory of its own.
That HOME holds the environments.yml file of the test, thus the test never
reads or writes the file of the developer.
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
from tests.integration.ci_snowflake import (
    IntegrationConfig,
    IntegrationConfigError,
    connect,
    create_run_schemas,
    drop_run_schemas,
    load_config,
    seed_source_tables,
)

PIPELINE_PROJECT_PATH = Path(__file__).parent / "pipeline_project"
ENVIRONMENT_NAME = "ci"


@pytest.fixture(scope="session")
def integration_config() -> IntegrationConfig:
    """Give the CI configuration, or skip the test.

    A developer without Snowflake credentials runs `uv run pytest` and gets a
    skip, not a failure.
    """
    try:
        return load_config()
    except IntegrationConfigError as error:
        pytest.skip(f"No Snowflake credentials for the integration tests: {error}")


@pytest.fixture(scope="session")
def snowflake_adapter(integration_config: IntegrationConfig) -> Iterator[SnowflakeAdapter]:
    """Open one Snowflake connection for the complete session."""
    adapter = connect(integration_config)
    try:
        yield adapter
    finally:
        adapter.close()


@pytest.fixture(scope="session")
def snowflake_workspace(
    snowflake_adapter: SnowflakeAdapter, integration_config: IntegrationConfig
) -> Iterator[IntegrationConfig]:
    """Write the seed tables, make the run schemas, and drop them at the end.

    The workflow drops the schemas again after the job. Thus a test that stops
    with a hard error still loses its schemas.
    """
    seed_source_tables(snowflake_adapter, integration_config)
    create_run_schemas(snowflake_adapter, integration_config)
    try:
        yield integration_config
    finally:
        drop_run_schemas(snowflake_adapter, integration_config)


@pytest.fixture(scope="session")
def clair_home(
    tmp_path_factory: pytest.TempPathFactory, integration_config: IntegrationConfig
) -> Path:
    """Make a HOME directory that holds the environments.yml of the tests."""
    home_dir = tmp_path_factory.mktemp("clair_home")
    clair_dir = home_dir / ".clair"
    clair_dir.mkdir()
    environments_file = clair_dir / "environments.yml"
    environments_file.write_text(integration_config.to_environments_yaml(ENVIRONMENT_NAME))
    environments_file.chmod(0o600)
    return home_dir


@pytest.fixture
def clair_environment(
    clair_home: Path, snowflake_workspace: IntegrationConfig
) -> dict[str, str]:
    """Give the environment variables of a clair subprocess."""
    return make_clair_environment(clair_home, snowflake_workspace)


def make_clair_environment(home_dir: Path, config: IntegrationConfig) -> dict[str, str]:
    """Build the environment of a clair subprocess.

    The function copies the current environment, then it replaces HOME and adds
    the CI variables. The routing file and the database config file read those
    variables.
    """
    environment = dict(os.environ)
    environment["HOME"] = str(home_dir)
    environment["USERPROFILE"] = str(home_dir)
    environment["CLAIR_ENV"] = ENVIRONMENT_NAME
    # JSON logs give the tests one event for each line, thus an assertion reads a
    # field and not a text that a new version of clair could format differently.
    environment["CLAIR_LOG_FORMAT"] = "json"
    environment["CLAIR_CI_SCHEMA_PREFIX"] = config.schema_prefix
    environment["CLAIR_CI_SNOWFLAKE_WAREHOUSE"] = config.warehouse
    environment["CLAIR_CI_SNOWFLAKE_ROLE"] = config.role
    return environment


def run_clair(
    arguments: list[str],
    environment: dict[str, str],
    stdin_text: str | None = None,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run the clair CLI as a subprocess and give the finished process.

    Args:
        arguments: The CLI arguments, for example ["run", "--project", "..."].
        environment: The environment variables of the subprocess.
        stdin_text: The text that answers the prompts of the command.
        expect_success: If true, a non-zero exit code fails the test.
    """
    completed = subprocess.run(
        [sys.executable, "-m", "clair.cli.main", *arguments],
        env=environment,
        input=stdin_text,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    if expect_success and completed.returncode != 0:
        pytest.fail(
            f"clair {' '.join(arguments)} gave the exit code {completed.returncode}\n"
            f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
        )
    return completed


def log_events(completed: subprocess.CompletedProcess[str]) -> list[dict[str, object]]:
    """Give the JSON log events that a clair subprocess wrote to stdout."""
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


def logical_names_of(
    completed: subprocess.CompletedProcess[str], event_name: str
) -> set[str]:
    """Give the logical name of each event with this name."""
    return {
        str(event["logical"])
        for event in log_events(completed)
        if event.get("event") == event_name and "logical" in event
    }
