"""The tests of tests.integration.config.

The schema name is mandatory. A run drops that schema before it starts, thus a
default name would let two runs delete the tables of each other.
"""

from __future__ import annotations

import pytest

from tests.integration.config import IntegrationConfigError, load_config

ACCOUNT_VARIABLE = "CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT"
PASSWORD_VARIABLE = "CLAIR_PR_TESTING_SNOWFLAKE_PASSWORD"
SCHEMA_VARIABLE = "CLAIR_PR_TESTING_SCHEMA_NAME"


@pytest.fixture
def connection_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the account and the password, and remove the schema name."""
    monkeypatch.setenv(ACCOUNT_VARIABLE, "an_account")
    monkeypatch.setenv(PASSWORD_VARIABLE, "a_password")
    monkeypatch.delenv(SCHEMA_VARIABLE, raising=False)
    monkeypatch.delenv("CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)


def test_an_absent_schema_name_stops_the_run(connection_variables: None) -> None:
    """load_config refuses to give a config with no schema name."""
    with pytest.raises(IntegrationConfigError, match=SCHEMA_VARIABLE):
        load_config()


def test_an_empty_schema_name_stops_the_run(
    connection_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A variable of spaces is the same as an absent variable."""
    monkeypatch.setenv(SCHEMA_VARIABLE, "   ")
    with pytest.raises(IntegrationConfigError, match=SCHEMA_VARIABLE):
        load_config()


def test_the_variable_gives_the_schema_name(
    connection_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The config holds the name of the variable, in lower case."""
    monkeypatch.setenv(SCHEMA_VARIABLE, "Local_Omer_Branch")
    assert load_config().schema_name == "local_omer_branch"


def test_the_argument_wins_against_the_variable(
    connection_variables: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cleanup command gives the name of the schema that it drops."""
    monkeypatch.setenv(SCHEMA_VARIABLE, "from_the_variable")
    assert load_config("from_the_argument").schema_name == "from_the_argument"


def test_the_argument_works_with_no_variable(connection_variables: None) -> None:
    """The cleanup command needs no variable."""
    assert load_config("from_the_argument").schema_name == "from_the_argument"


def test_a_protected_schema_name_stops_the_run(connection_variables: None) -> None:
    """A golden schema holds the source tables, thus a run must not name it."""
    with pytest.raises(IntegrationConfigError, match="protected"):
        load_config("example_1")
