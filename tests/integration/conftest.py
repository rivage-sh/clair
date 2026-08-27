"""Fixtures for the integration tests.

The tests call the Python API of clair in this process: `clair.run()`,
`clair.test()` and `clair.compile()`. The result object then tells a test what
happened -- the statements, the addresses, the effective run mode, the test
results -- and a test reads no log line to learn it.

Clair still reads the environment variables of the process. The
`clair_environment` fixture sets them for the session: a private HOME that holds
`environments.yml`, the environment name, and the schema name that the test
routing entry reads.

Without the Snowflake settings the tests skip on a workstation, and they fail in
GitHub Actions. A run with no credentials in CI would otherwise report success
after it ran nothing.
"""

from __future__ import annotations

import os
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


@pytest.fixture(scope="session")
def integration_config() -> IntegrationConfig:
    """Give the settings of the run, or stop the tests."""
    try:
        return load_config()
    except IntegrationConfigError as error:
        # The integration workflow sets this variable. The other CI jobs run the
        # same files with no credentials, and those must skip, not fail.
        if os.environ.get("CLAIR_PR_TESTING_REQUIRE_SNOWFLAKE"):
            pytest.fail(f"The integration job needs the Snowflake settings: {error}")
        pytest.skip(str(error))


@pytest.fixture(scope="session")
def snowflake_workspace(integration_config: IntegrationConfig) -> IntegrationConfig:
    """Give one empty schema, with the source tables of each example project.

    The run starts with a drop. A second commit of one pull request reuses the
    schema name of the first, and a Trouve that the commit deleted would stay.

    The run does not drop the schema at the end, thus you can read the tables of
    a failed run. `.github/workflows/integration-clean-up.yml` drops the schema
    when the pull request closes.
    """
    adapter = connect(integration_config)
    try:
        drop_schema(adapter, integration_config.schema_name)
    finally:
        adapter.close()

    prepare(integration_config)
    return integration_config


@pytest.fixture(scope="session")
def adapter(snowflake_workspace: IntegrationConfig) -> Iterator[SnowflakeAdapter]:
    """Give one open connection for the assertions.

    This connection belongs to the tests. Each `clair.run()` call opens its own
    connection, exactly as a run on a workstation does.
    """
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


@pytest.fixture(scope="session")
def clair_environment(
    snowflake_workspace: IntegrationConfig, clair_home: Path
) -> Iterator[IntegrationConfig]:
    """Point clair at the private HOME and at the schema of the run.

    A test that calls the Python API asks for this fixture first. `clair.run()`
    then finds `environments.yml` under HOME, and the routing entry of the
    project copy reads CLAIR_PR_TESTING_SCHEMA_NAME.
    """
    variables = pytest.MonkeyPatch()
    variables.setenv("HOME", str(clair_home))
    variables.setenv("USERPROFILE", str(clair_home))
    variables.setenv("CLAIR_ENV", ENVIRONMENT_NAME)
    variables.setenv("CLAIR_PR_TESTING_SCHEMA_NAME", snowflake_workspace.schema_name)
    variables.setenv(
        "CLAIR_PR_TESTING_SNOWFLAKE_WAREHOUSE", snowflake_workspace.warehouse
    )
    variables.setenv("CLAIR_PR_TESTING_SNOWFLAKE_ROLE", snowflake_workspace.role)
    yield snowflake_workspace
    variables.undo()
