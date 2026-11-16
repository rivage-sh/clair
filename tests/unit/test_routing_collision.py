"""Two Trouves that route to one physical address stop the command.

A DAG node holds one Trouve, keyed by the physical address. A collision would
thus drop one Trouve from the run, and each reader of it would read the other
Trouve. `clair validate`, `clair compile` and `clair run` apply one rule, and a
SOURCE counts: a Table that routes onto a Source replaces the data it reads.
"""

from __future__ import annotations

import pytest

import clair
from clair.exceptions import RoutingCollisionError
from tests.helpers import write_project_files

# Each Trouve of the schema `one` goes to the schema `two`.
MERGING_ROUTING = """\
from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class MergingRouting(RoutingEntry):
    environment_name: str = "dev"

    def route(self, trouve_address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        if trouve_address.schema_name == "one":
            return trouve_address.model_copy(update={"schema_name": "two"})
        return trouve_address


routing = RoutingTable(entries=[MergingRouting()])
"""

TABLE_FILE = 'from clair import Table\ntrouve = Table(sql="select 1 as n")\n'
SOURCE_FILE = "from clair import Source\ntrouve = Source()\n"


@pytest.fixture(params=["table", "source"])
def colliding_project(request, tmp_path):
    """db.one.daily (a Table) routes onto db.two.daily, a Table or a Source."""
    second_file = TABLE_FILE if request.param == "table" else SOURCE_FILE
    (tmp_path / "__routing__.py").write_text(MERGING_ROUTING)
    return write_project_files(
        tmp_path, {"db/one/daily.py": TABLE_FILE, "db/two/daily.py": second_file}
    )


def test_compile_stops_and_names_each_trouve(colliding_project):
    with pytest.raises(RoutingCollisionError) as raised:
        clair.compile(colliding_project, env="dev")

    assert raised.value.collisions == [("db.two.daily", ["db.one.daily", "db.two.daily"])]


def test_validate_reports_the_same_collision(colliding_project):
    report = clair.validate(colliding_project, env="dev")

    assert [
        (collision.physical_address, collision.logical_addresses)
        for collision in report.collisions
    ] == [("db.two.daily", ["db.one.daily", "db.two.daily"])]


def test_an_environment_without_the_routing_entry_has_no_collision(colliding_project):
    output = clair.compile(colliding_project, env="prod")

    assert {str(node.addresses.logical) for node in output.compiled_nodes} >= {"db.one.daily"}
