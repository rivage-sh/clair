"""The role and the warehouse of one Trouve never stay for the next Trouve.

A thread runs many Trouves on one session, and a ``USE ROLE`` stays until the
next one. A Trouve that sets ``role`` must thus not give its role to the Trouve
after it on the same thread: that Trouve runs with the role of the session, and
the owner of the tables that it makes does not depend on the schedule.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from tests.helpers import (
    FakeConnection,
    FakeCursor,
    RecordingAdapter,
    make_environment,
    write_project_marker,
)

OPENING_ROLE = "TRANSFORMER"
OPENING_WAREHOUSE = "COMPUTE_WH"


def _parameter_row(name: str, value: str, level: str) -> tuple[str, ...]:
    """Give a row of SHOW PARAMETERS: (key, value, default, level, description, type)."""
    return (name, value, "", level, "", "STRING")


def _adapter_with_opening_context(
    query_tag: str | None = None, timeout_seconds: int | None = None
) -> tuple[SnowflakeAdapter, FakeCursor]:
    """Give an adapter whose session opens with the role, the warehouse and the parameters.

    A parameter of None is not set on the session: SHOW PARAMETERS gives the
    value of the account, and an empty level.
    """
    adapter = SnowflakeAdapter()
    tag_row = _parameter_row(
        "QUERY_TAG", query_tag or "", "" if query_tag is None else "SESSION"
    )
    timeout_row = _parameter_row(
        "STATEMENT_TIMEOUT_IN_SECONDS",
        str(timeout_seconds or 172800),
        "" if timeout_seconds is None else "SESSION",
    )
    cursor = FakeCursor(
        [],
        answers={
            "SELECT CURRENT_ROLE()": [(OPENING_ROLE, OPENING_WAREHOUSE)],
            "SHOW PARAMETERS LIKE 'QUERY_TAG'": [tag_row],
            "SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS'": [timeout_row],
        },
    )
    adapter._conn = cast(Any, FakeConnection(cursor))
    return adapter, cursor


def _session_changes(cursor: FakeCursor) -> list[str]:
    """Give each statement that changes the session, and leave out each read."""
    return [sql for sql in cursor.executed if sql.startswith(("ALTER SESSION", "USE "))]


def _use_statements(cursor: FakeCursor) -> list[str]:
    return [sql for sql in cursor.executed if sql.startswith("USE ")]


@pytest.mark.parametrize(
    ("trouve_role", "trouve_warehouse", "expected"),
    [
        (None, None, [f"USE ROLE {OPENING_ROLE}", f"USE WAREHOUSE {OPENING_WAREHOUSE}"]),
        ("PII_ROLE", None, ["USE ROLE PII_ROLE", f"USE WAREHOUSE {OPENING_WAREHOUSE}"]),
        (None, "XL_WH", [f"USE ROLE {OPENING_ROLE}", "USE WAREHOUSE XL_WH"]),
        ("  ", "", [f"USE ROLE {OPENING_ROLE}", f"USE WAREHOUSE {OPENING_WAREHOUSE}"]),
    ],
)
def test_a_trouve_after_another_gets_its_own_context(trouve_role, trouve_warehouse, expected):
    adapter, cursor = _adapter_with_opening_context()
    adapter.set_context(role="OTHER_ROLE", warehouse="OTHER_WH")
    cursor.executed.clear()

    adapter.set_context(role=trouve_role, warehouse=trouve_warehouse)

    assert _use_statements(cursor) == expected


def test_the_adapter_reads_the_opening_context_one_time_before_any_use():
    adapter, cursor = _adapter_with_opening_context()

    adapter.set_context(role="PII_ROLE")
    adapter.set_context()

    reads = [sql for sql in cursor.executed if "CURRENT_ROLE()" in sql]
    assert len(reads) == 1
    assert cursor.executed[0] == reads[0]


def test_a_database_name_of_none_sends_no_use_database():
    adapter, cursor = _adapter_with_opening_context()

    adapter.set_context()

    assert not any(sql.startswith("USE DATABASE") for sql in cursor.executed)


def _write_project(project_root: Path) -> Path:
    write_project_marker(project_root)
    (project_root / "db" / "pii").mkdir(parents=True)
    (project_root / "db" / "pii" / "customers.py").write_text(
        'from clair import Table\ntrouve = Table(sql="select 1 as id", role="PII_ROLE")\n'
    )
    (project_root / "db" / "pii" / "customer_count.py").write_text(
        "from clair import Table\n"
        "from db.pii.customers import trouve as customers\n"
        'trouve = Table(sql=f"select count(*) as n from {customers}")\n'
    )
    return project_root


def test_the_runner_sets_the_context_of_each_trouve_even_with_no_role(tmp_path):
    # The profile names no role, thus the second Trouve resolves no role. The
    # runner still sets its context, and None means the role of the session.
    project_root = _write_project(tmp_path / "project")
    adapter = RecordingAdapter()

    clair.run(project_root, env=make_environment(), adapter=adapter, threads=1)

    assert [context["role"] for context in adapter.contexts] == ["PII_ROLE", None]


def _write_tested_project(project_root: Path) -> Path:
    write_project_marker(project_root)
    (project_root / "db" / "pii").mkdir(parents=True)
    (project_root / "db" / "pii" / "customers.py").write_text(
        "from clair import Table, TestNotNull\n"
        'trouve = Table(sql="select 1 as id", role="PII_ROLE", warehouse="XL_WH",\n'
        '               statement_timeout_seconds=60, tests=[TestNotNull(column="id")])\n'
    )
    (project_root / "db" / "pii" / "customer_count.py").write_text(
        "from clair import Table, TestNotNull\n"
        "from db.pii.customers import trouve as customers\n"
        'trouve = Table(sql=f"select count(*) as n from {customers}", tests=[TestNotNull(column="n")])\n'
    )
    (project_root / "db" / "pii" / "untested.py").write_text(
        'from clair import Table\ntrouve = Table(sql="select 1 as id", role="OTHER_ROLE")\n'
    )
    return project_root


def test_clair_test_sets_the_session_of_each_trouve_that_it_tests(tmp_path):
    project_root = _write_tested_project(tmp_path / "project")
    adapter = RecordingAdapter()

    clair.test(project_root, env=make_environment(), adapter=adapter, threads=1)

    # A Trouve with no test sends nothing, thus it sets no session. The second
    # Trouve gets the warehouse of the profile, and the role of the session.
    assert [(context["role"], context["warehouse"]) for context in adapter.contexts] == [
        ("PII_ROLE", "XL_WH"),
        (None, make_environment().connection.warehouse),
    ]
    tags = [json.loads(query_tag) for query_tag, _timeout in adapter.session_parameters]
    assert [tag["trouve"] for tag in tags] == ["db.pii.customers", "db.pii.customer_count"]
    assert len({tag["run_id"] for tag in tags}) == 1
    assert [timeout for _tag, timeout in adapter.session_parameters] == [60, None]


@pytest.mark.parametrize("operation", [clair.run, clair.test], ids=["run", "test"])
@pytest.mark.parametrize("threads", [1, 2])
def test_the_adapter_of_the_caller_gets_its_session_back(tmp_path, operation, threads):
    # The caller keeps the ownership of the adapter that it gives. After the
    # operation, its next query must not run with the role, the warehouse, the
    # query tag or the timeout of the last Trouve.
    project_root = _write_tested_project(tmp_path / "project")
    adapter = RecordingAdapter()

    operation(project_root, env=make_environment(), adapter=adapter, threads=threads)

    if threads == 1:
        assert adapter.session_parameters, "the operation set no session on the adapter"
    assert adapter.session_reset_positions == [len(adapter.session_parameters)]


def test_the_snowflake_adapter_gives_back_the_opening_session():
    adapter, cursor = _adapter_with_opening_context()
    adapter.set_session_parameters(query_tag='{"trouve": "db.pii.customers"}', statement_timeout_seconds=60)
    adapter.set_context(role="PII_ROLE", warehouse="XL_WH")
    cursor.executed.clear()

    adapter.reset_session()

    assert _session_changes(cursor) == [
        "ALTER SESSION UNSET QUERY_TAG",
        "ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS",
        f"USE ROLE {OPENING_ROLE}",
        f"USE WAREHOUSE {OPENING_WAREHOUSE}",
    ]


def test_the_snowflake_adapter_gives_back_the_query_tag_and_the_timeout_of_the_caller():
    # A notebook can tag its own session. After a run, its next query must
    # carry its own tag and timeout, and not the values of the account.
    adapter, cursor = _adapter_with_opening_context(query_tag="notebook", timeout_seconds=600)
    adapter.set_session_parameters(query_tag='{"trouve": "db.pii.customers"}', statement_timeout_seconds=60)
    adapter.set_context(role="PII_ROLE")
    cursor.executed.clear()

    adapter.reset_session()

    assert _session_changes(cursor)[:2] == [
        "ALTER SESSION SET QUERY_TAG = 'notebook'",
        "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600",
    ]


def test_a_use_role_of_the_caller_between_two_runs_stays_after_the_second_run():
    # The opening role is the role before each run, and not before the first
    # run: the caller can change it between two runs.
    adapter, cursor = _adapter_with_opening_context()
    adapter.set_context(role="PII_ROLE")
    adapter.reset_session()
    cursor.answers["SELECT CURRENT_ROLE()"] = [("ANALYST", OPENING_WAREHOUSE)]
    cursor.executed.clear()

    adapter.set_context()
    adapter.reset_session()

    assert [sql for sql in _session_changes(cursor) if sql.startswith("USE ROLE")] == [
        "USE ROLE ANALYST",
        "USE ROLE ANALYST",
    ]


def test_the_snowflake_adapter_changes_nothing_when_clair_set_no_session():
    adapter, cursor = _adapter_with_opening_context()

    adapter.reset_session()

    assert cursor.executed == []
