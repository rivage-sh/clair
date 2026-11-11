"""Each Snowflake session of a run stays open for the complete run.

Snowflake ends a session after 4 hours with no query. The pool opens each
connection when the run starts, and a thread can wait longer than that for a
slow Trouve upstream. Thus each connection asks the connector for a heartbeat.
"""

from __future__ import annotations

import pytest
import snowflake.connector

from clair import Authenticator, Password, SnowflakeAdapter
from tests.helpers import make_environment


@pytest.mark.parametrize(
    "authentication",
    [Authenticator(authenticator="externalbrowser"), Password(password="secret")],
    ids=["sso", "password"],
)
def test_each_connection_of_a_run_keeps_its_session_alive(monkeypatch, authentication):
    connect_arguments: list[dict[str, object]] = []

    def fake_connect(**arguments: object) -> object:
        connect_arguments.append(arguments)
        return object()

    monkeypatch.setattr(snowflake.connector, "connect", fake_connect)

    adapter = SnowflakeAdapter.connected(make_environment(authentication=authentication))
    adapter.new_connection()
    adapter.new_connection()

    assert [arguments["client_session_keep_alive"] for arguments in connect_arguments] == [True] * 3
