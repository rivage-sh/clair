"""Fixtures for the integration tests.

The tests call the Python API of clair in this process: `clair.run()`,
`clair.test()` and `clair.compile()`. The result object then tells a test what
happened -- the statements, the addresses, the effective run mode, the test
results -- and a test reads no log line to learn it.

`clair.run()`, `clair.test()` and `clair.compile()` accept the parsed
Environment, thus the tests write no `environments.yml`, and they need no
private HOME. The `environment` fixture makes the object. The
`clair_environment` fixture sets the variables that the test routing entry
reads: the schema name, the warehouse and the role.

Without the Snowflake settings the tests skip on a workstation, and they fail in
GitHub Actions. A run with no credentials in CI would otherwise report success
after it ran nothing.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from clair.environments.environments import Environment
from tests.integration.config import (
    IntegrationConfig,
    IntegrationConfigError,
    load_config,
)
from tests.integration.routing_rule import TABLE_PREFIX_VARIABLE, workspace_prefix_of
from tests.integration.setup import create_schema, load_source_tables
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
    """Give the schema of the run.

    This fixture drops nothing. A parallel run starts one worker process for
    each group of tests, thus each worker runs this fixture. A drop here would
    remove the tables of a worker that still runs. The `Prepare the schema` step
    of `.github/workflows/integration.yml` drops the schema one time, before
    pytest starts.

    The run does not drop the schema at the end, thus you can read the tables of
    a failed run. `.github/workflows/integration-clean-up.yml` drops the schema
    when the pull request closes.
    """
    adapter = connect(integration_config)
    try:
        create_schema(adapter, integration_config.schema_name)
    finally:
        adapter.close()
    return integration_config


@pytest.fixture(scope="class", autouse=True)
def workspace_prefix(request: pytest.FixtureRequest) -> Iterator[str]:
    """Give each test class its own table names inside the schema of the run.

    The module name and the class name become the prefix of each table that the
    class builds. Two classes that build the same example project then write two
    different tables, and they can run at the same time.

    The prefix comes from the code, thus a new test class is isolated with no
    action from the author. A name that a person selects can repeat.

    `--dist loadscope` sends each class to one worker, thus one class holds one
    prefix in one process.
    """
    prefix = workspace_prefix_of(
        request.module.__name__.rsplit(".", 1)[-1],
        request.cls.__name__ if request.cls is not None else None,
    )
    variables = pytest.MonkeyPatch()
    variables.setenv(TABLE_PREFIX_VARIABLE, prefix)
    yield prefix
    variables.undo()


@pytest.fixture(scope="class")
def example_sources(
    snowflake_workspace: IntegrationConfig, workspace_prefix: str
) -> list[str]:
    """Clone the golden source table of each example project into the workspace.

    A clone is a zero copy operation, thus each class can hold its own copy. The
    name of each copy holds `workspace_prefix`, so this fixture runs one time
    for each class, and not one time for the session.

    A probe project of the staging tests and the seed tests makes its own rows.
    Only a test of a project in `examples/projects/` asks for this fixture.
    """
    adapter = connect(snowflake_workspace)
    try:
        return load_source_tables(adapter, snowflake_workspace.schema_name)
    finally:
        adapter.close()


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
def environment(snowflake_workspace: IntegrationConfig) -> Environment:
    """Give the Environment that each API call takes.

    A test gives this object to `clair.run()`, exactly as a notebook or a
    service does.
    """
    return snowflake_workspace.to_environment()


@pytest.fixture(scope="session")
def clair_environment(
    snowflake_workspace: IntegrationConfig,
) -> Iterator[IntegrationConfig]:
    """Point the project routing entry at the schema of the run.

    A test that calls the Python API asks for this fixture first. The routing
    entry of the project copy reads CLAIR_PR_TESTING_SCHEMA_NAME.
    """
    variables = pytest.MonkeyPatch()
    variables.setenv("CLAIR_PR_TESTING_SCHEMA_NAME", snowflake_workspace.schema_name)
    variables.setenv(
        "CLAIR_PR_TESTING_SNOWFLAKE_WAREHOUSE", snowflake_workspace.warehouse
    )
    variables.setenv("CLAIR_PR_TESTING_SNOWFLAKE_ROLE", snowflake_workspace.role)
    yield snowflake_workspace
    variables.undo()
