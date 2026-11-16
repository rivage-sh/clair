"""A connection that clair cannot open raises a ClairError.

`clair.run()` and `clair.test()` raise a ClairError when the connection fails,
and the CLI turns a ClairError into one error line. A fault of the connector or
of the key file must not escape as a different type, because the user then
reads a traceback.
"""

from __future__ import annotations

import pytest
import snowflake.connector
from snowflake.connector.errors import DatabaseError

from clair import Password, PrivateKey, SnowflakeAdapter
from clair.exceptions import WarehouseConnectionError
from tests.helpers import make_environment


def _refuse_login(**_arguments: object) -> object:
    raise DatabaseError("Incorrect username or password was specified.")


@pytest.mark.parametrize(
    ("authentication", "cause_name"),
    [
        pytest.param(Password(password="wrong"), "DatabaseError", id="the_connector_refuses"),
        pytest.param(
            PrivateKey(private_key_path="/nonexistent/key.p8"),
            "FileNotFoundError",
            id="the_key_file_is_absent",
        ),
    ],
)
def test_a_connection_fault_is_a_clair_error(monkeypatch, authentication, cause_name):
    monkeypatch.setattr(snowflake.connector, "connect", _refuse_login)
    environment = make_environment(authentication=authentication)

    with pytest.raises(WarehouseConnectionError) as raised:
        SnowflakeAdapter.connected(environment)

    assert raised.value.env_name == environment.name
    assert type(raised.value.__cause__).__name__ == cause_name
