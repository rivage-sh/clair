"""The role and the warehouse of one Trouve never stay for the next Trouve.

A thread runs many Trouves on one session, and a ``USE ROLE`` stays until the
next one. A Trouve that sets ``role`` must thus not give its role to the Trouve
after it on the same thread: that Trouve runs with the role of the session, and
the owner of the tables that it makes does not depend on the schedule.
"""

from __future__ import annotations

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


def _adapter_with_opening_context() -> tuple[SnowflakeAdapter, FakeCursor]:
    adapter = SnowflakeAdapter()
    cursor = FakeCursor([(OPENING_ROLE, OPENING_WAREHOUSE)])
    adapter._conn = cast(Any, FakeConnection(cursor))
    return adapter, cursor


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
