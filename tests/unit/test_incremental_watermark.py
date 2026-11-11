"""``{THIS}`` in the SQL of a Trouve reads the table that the Trouve writes.

An incremental filter compares the new rows with the newest row of the target:
``where created_at > (select max(created_at) from {THIS})``. The marker must
give the physical address, because routing moves the table that the run writes,
and it must make no DAG edge from the Trouve to itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair import RunMode
from clair.core.runner import RunSuccess
from tests.helpers import RecordingAdapter, make_environment, write_project_marker

EXAMPLE_3 = Path(__file__).parents[2] / "examples" / "projects" / "example_3"

SUFFIX_ROUTING = """
from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType

class Suffix(RoutingEntry):
    def route(self, address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return address
        return address.model_copy(update={"database_name": address.database_name + "_dev"})

routing = RoutingTable(entries=[Suffix(environment_name="dev")])
"""

WATERMARK_TROUVE = """
import clair
from clair import THIS, Append, Incremental, RunMode, Table
from db.raw.events import trouve as events

sql = f"select * from {events}"
if clair.run_mode == RunMode.INCREMENTAL:
    sql += f" where loaded_at > (select max(loaded_at) from {THIS})"
trouve = Table(sql=sql, run_config=Incremental(strategy=Append()))
"""

ROUTED_FILTER = "where loaded_at > (select max(loaded_at) from db_dev.derived.events)"


@pytest.fixture
def watermark_project(tmp_path: Path) -> Path:
    project_root = write_project_marker(tmp_path / "project")
    (project_root / "__routing__.py").write_text(SUFFIX_ROUTING)
    (project_root / "db" / "raw").mkdir(parents=True)
    (project_root / "db" / "derived").mkdir(parents=True)
    (project_root / "db" / "raw" / "events.py").write_text("from clair import Source\ntrouve = Source()\n")
    (project_root / "db" / "derived" / "events.py").write_text(WATERMARK_TROUVE)
    return project_root


def test_this_gives_the_routed_physical_address_and_no_self_edge(watermark_project):
    output = clair.compile(watermark_project, env="dev", run_mode=RunMode.INCREMENTAL)

    (node,) = [node for node in output.compiled_nodes if str(node.addresses.logical) == "db.derived.events"]
    assert any(ROUTED_FILTER in statement for statement in node.sql)
    assert node.dependencies == ["db.raw.events"]


def test_a_staged_run_inserts_into_the_staging_copy_and_reads_the_physical_table(watermark_project):
    adapter = RecordingAdapter()

    summary = clair.run(
        watermark_project, env=make_environment(), run_mode=RunMode.INCREMENTAL, adapter=adapter
    )

    result = summary.result("db.derived.events")
    assert isinstance(result, RunSuccess)
    (insert,) = [statement.sql for statement in result.statements if statement.sql.startswith("INSERT")]
    assert insert.startswith("INSERT INTO db_dev.derived.events__clair_")
    assert ROUTED_FILTER in insert


@pytest.mark.parametrize(
    ("table_name", "expected_filter"),
    [
        ("recent_orders", "where created_at > (select max(created_at) from alice.derived.recent_orders)"),
        (
            "daily_orders",
            "where created_date >= dateadd('day', -3, (select max(created_date) from alice.derived.daily_orders))",
        ),
    ],
)
def test_the_documented_examples_filter_on_their_own_table(monkeypatch, table_name, expected_filter):
    monkeypatch.setenv("CLAIR_USER", "alice")

    output = clair.compile(EXAMPLE_3, env="dev", run_mode=RunMode.INCREMENTAL)

    (node,) = [
        node
        for node in output.compiled_nodes
        if str(node.addresses.logical) == f"example_3_database.derived.{table_name}"
    ]
    assert any(expected_filter in statement for statement in node.sql)
