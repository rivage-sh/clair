"""Snowflake stops the statement that a cancelled run waits for.

``SnowflakeAdapter.cancel()`` names the statement by its text and its request
id, through the connector. Only Snowflake can tell that the cancel reaches it,
thus this test waits for a statement that runs for a long time, and cancels it.
"""

from __future__ import annotations

import threading
import time

import pytest

from clair.adapters.base import Statement
from clair.adapters.snowflake import SnowflakeAdapter

pytestmark = pytest.mark.integration

WAIT_SECONDS = 60


def test_a_cancel_stops_a_statement_in_snowflake(adapter: SnowflakeAdapter):
    connection = adapter.new_connection()
    results: list[Statement] = []
    worker = threading.Thread(
        target=lambda: results.append(connection.execute(f"SELECT SYSTEM$WAIT({WAIT_SECONDS})"))
    )
    try:
        start = time.monotonic()
        worker.start()
        # Give the statement time to reach Snowflake.
        time.sleep(3)
        connection.cancel()
        worker.join(timeout=WAIT_SECONDS)

        (statement,) = results
        assert not statement.success
        assert time.monotonic() - start < WAIT_SECONDS / 2
    finally:
        connection.close()
