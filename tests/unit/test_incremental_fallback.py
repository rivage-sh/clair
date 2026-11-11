"""A node that makes its object again uses the SQL of the full refresh mode.

A module reads ``clair.run_mode`` at import time and adds an incremental filter.
An incremental run can still make an object again from nothing: the table does
not exist yet, the address holds a view, the Trouve is a VIEW, or its run_config
is ``FullRefresh``. That build must not keep the filter, because the object then
holds the filtered rows only, and the run reports success.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair import RunMode
from clair.core.runner import RunFailure, RunSkip, RunSuccess, RunSummary
from clair.exceptions import RunError
from tests.helpers import RecordingAdapter, make_environment, write_project_marker

INCREMENTAL_FILTER = "created_at > dateadd('day', -3, current_timestamp())"
TARGET = "db.derived.recent_orders"
DOWNSTREAM = "db.derived.order_summary"


def _filtered_trouve_file(trouve_expression: str) -> str:
    """Give a Trouve file whose SQL adds the incremental filter in the incremental mode."""
    return f"""
import clair
from clair import Append, DeleteInsert, FullRefresh, Incremental, RunMode, Table, Upsert, View, Column
from db.raw.orders import trouve as orders

sql = f"select * from {{orders}}"
if clair.run_mode == RunMode.INCREMENTAL:
    sql += " where {INCREMENTAL_FILTER}"
trouve = {trouve_expression}
"""


def _write_project(project_root: Path, trouve_expression: str) -> Path:
    """Write a project: a source, the filtered Trouve, and a Trouve downstream of it."""
    write_project_marker(project_root)
    (project_root / "db" / "raw").mkdir(parents=True)
    (project_root / "db" / "derived").mkdir(parents=True)
    (project_root / "db" / "raw" / "orders.py").write_text(
        "from clair import Source\ntrouve = Source()\n"
    )
    (project_root / "db" / "derived" / "recent_orders.py").write_text(
        _filtered_trouve_file(trouve_expression)
    )
    (project_root / "db" / "derived" / "order_summary.py").write_text(
        "from clair import Table\n"
        "from db.derived.recent_orders import trouve as recent_orders\n"
        'trouve = Table(sql=f"select count(*) as n from {recent_orders}")\n'
    )
    return project_root


APPEND = "Table(sql=sql, run_config=Incremental(strategy=Append()))"
UPSERT = (
    "Table(sql=sql, run_config=Incremental(strategy=Upsert(primary_key_columns=['id'])),"
    " columns=[Column(name='id', type='NUMBER')])"
)
DELETE_INSERT = "Table(sql=sql, run_config=Incremental(strategy=DeleteInsert(range_column='created_at')))"


def _build_sql_of(result: RunSuccess | RunFailure) -> str:
    """Give the statement that builds the object of the Trouve."""
    (build,) = [
        statement.sql
        for statement in result.statements
        if "select * from db.raw.orders" in statement.sql
    ]
    return build


def _run(project_root: Path, adapter: RecordingAdapter, run_mode: RunMode) -> RunSummary:
    return clair.run(project_root, env=make_environment(), run_mode=run_mode, adapter=adapter)


@pytest.mark.parametrize("trouve_expression", [APPEND, UPSERT, DELETE_INSERT])
def test_the_first_incremental_run_builds_each_row(tmp_path, trouve_expression):
    project_root = _write_project(tmp_path / "project", trouve_expression)

    summary = _run(project_root, RecordingAdapter(existing_tables=[]), RunMode.INCREMENTAL)

    result = summary.result(TARGET)
    assert isinstance(result, RunSuccess)
    assert result.effective_run_mode == RunMode.FULL_REFRESH
    build_sql = _build_sql_of(result)
    assert build_sql.startswith("CREATE OR REPLACE TABLE")
    assert INCREMENTAL_FILTER not in build_sql


@pytest.mark.parametrize("trouve_expression", [APPEND, UPSERT, DELETE_INSERT])
def test_an_incremental_run_on_a_table_that_exists_keeps_the_filter(tmp_path, trouve_expression):
    project_root = _write_project(tmp_path / "project", trouve_expression)

    summary = _run(project_root, RecordingAdapter(), RunMode.INCREMENTAL)

    result = summary.result(TARGET)
    assert isinstance(result, RunSuccess)
    assert result.effective_run_mode == RunMode.INCREMENTAL
    assert INCREMENTAL_FILTER in _build_sql_of(result)


def test_a_full_refresh_run_never_holds_the_filter(tmp_path):
    project_root = _write_project(tmp_path / "project", APPEND)

    summary = _run(project_root, RecordingAdapter(), RunMode.FULL_REFRESH)

    result = summary.result(TARGET)
    assert isinstance(result, RunSuccess)
    assert INCREMENTAL_FILTER not in _build_sql_of(result)


def test_a_table_that_was_a_view_builds_its_candidate_with_each_row(tmp_path):
    # The address holds a view, thus the incremental run has no base table. The
    # candidate is a full refresh, and the promotion then stops on the type.
    project_root = _write_project(tmp_path / "project", APPEND)
    adapter = RecordingAdapter(existing_views=[TARGET])

    summary = _run(project_root, adapter, RunMode.INCREMENTAL)

    result = summary.result(TARGET)
    assert isinstance(result, RunFailure)
    assert result.effective_run_mode == RunMode.FULL_REFRESH
    assert INCREMENTAL_FILTER not in _build_sql_of(result)


@pytest.mark.parametrize(
    ("trouve_expression", "existing_views"),
    [
        ("View(sql=sql)", [TARGET]),
        ("Table(sql=sql)", []),
        ("Table(sql=sql, run_config=FullRefresh())", []),
    ],
    ids=["view", "table_with_default_run_config", "table_with_full_refresh"],
)
def test_a_trouve_with_no_incremental_mode_ignores_the_run_mode_of_the_run(
    tmp_path, trouve_expression, existing_views
):
    project_root = _write_project(tmp_path / "project", trouve_expression)

    summary = _run(project_root, RecordingAdapter(existing_views=existing_views), RunMode.INCREMENTAL)

    result = summary.result(TARGET)
    assert isinstance(result, RunSuccess)
    assert all(INCREMENTAL_FILTER not in statement.sql for statement in result.statements)


def test_the_compiled_plan_shows_the_sql_that_the_run_sends(tmp_path):
    # A view never runs incrementally, thus the plan of an incremental compile
    # shows its full refresh SQL, as the run sends it.
    project_root = _write_project(tmp_path / "project", "View(sql=sql)")

    output = clair.compile(project_root, env="dev", run_mode=RunMode.INCREMENTAL)

    (node,) = [node for node in output.compiled_nodes if str(node.addresses.logical) == TARGET]
    assert all(INCREMENTAL_FILTER not in statement for statement in node.sql)


def test_the_sql_of_the_full_refresh_mode_reads_the_addresses_of_the_selection(tmp_path):
    # The second import must resolve each reference with the same rule as the
    # first: an upstream Trouve that the run builds reads its physical address.
    project_root = _write_project(tmp_path / "project", APPEND)
    (project_root / "__routing__.py").write_text(
        "from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType\n"
        "class Suffix(RoutingEntry):\n"
        "    def route(self, address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:\n"
        "        if trouve_type == TrouveType.SOURCE:\n"
        "            return address\n"
        "        return address.model_copy(update={'database_name': address.database_name + '_dev'})\n"
        "routing = RoutingTable(entries=[Suffix(environment_name='dev')])\n"
    )

    summary = _run(project_root, RecordingAdapter(existing_tables=[]), RunMode.INCREMENTAL)

    downstream = summary.result(DOWNSTREAM)
    assert isinstance(downstream, RunSuccess)
    (build,) = [s.sql for s in downstream.statements if "count(*)" in s.sql]
    assert "from db_dev.derived.recent_orders" in build


def test_a_warehouse_that_cannot_describe_the_target_fails_the_node_and_writes_nothing(tmp_path):
    # "No answer" must not mean "no table": that path makes the table again.
    project_root = _write_project(tmp_path / "project", APPEND)
    adapter = RecordingAdapter(describe_error=RunError("Network unreachable"))

    summary = _run(project_root, adapter, RunMode.INCREMENTAL)

    result = summary.result(TARGET)
    assert isinstance(result, RunFailure)
    assert "Network unreachable" in result.error
    assert result.statements == []
    assert not any("recent_orders" in sql and "CREATE" in sql for sql in adapter.record.statements)
    assert isinstance(summary.result(DOWNSTREAM), RunSkip)
