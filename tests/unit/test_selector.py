"""The tests of the selector, the glob match, and the graph operators."""

import pytest

import clair
from clair.core.selector import (
    ParsedSelector,
    expand_selector,
    expand_selectors,
    parse_selector,
)
from clair.trouves.trouve_type import TrouveType
from tests.helpers import build_dag_of, write_project_marker

# A routing entry that moves each table to another database. A pattern names
# the logical address, thus it selects the same Trouves in each environment.
_SUFFIX_ROUTING = """
from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType

class Suffix(RoutingEntry):
    def route(self, address: TrouveAddress, trouve_type: TrouveType) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return address
        return address.model_copy(update={"database_name": address.database_name + "_alice"})

routing = RoutingTable(entries=[Suffix(environment_name="dev")])
"""


@pytest.fixture
def routed_project(tmp_path):
    project_root = write_project_marker(tmp_path / "project")
    (project_root / "__routing__.py").write_text(_SUFFIX_ROUTING)
    for schema_name in ("raw", "derived"):
        (project_root / "db" / schema_name).mkdir(parents=True)
    (project_root / "db" / "raw" / "events.py").write_text("from clair import Source\ntrouve = Source()\n")
    for table_name in ("daily", "weekly"):
        (project_root / "db" / "derived" / f"{table_name}.py").write_text(
            "from clair import Table\n"
            "from db.raw.events import trouve as events\n"
            'trouve = Table(sql=f"select * from {events}")\n'
        )
    return project_root


@pytest.mark.parametrize(
    ("select", "exclude", "expected_physical"),
    [
        (["db.derived.daily"], None, ["db_alice.derived.daily"]),
        (["db.derived.*"], ["db.derived.weekly"], ["db_alice.derived.daily"]),
        (["+db.derived.weekly"], None, ["db_alice.derived.weekly"]),
        # The physical address is not a name that a pattern matches.
        (["db_alice.derived.daily"], None, []),
    ],
)
def test_a_routed_environment_selects_by_the_logical_address(
    routed_project, select, exclude, expected_physical
):
    output = clair.compile(routed_project, env="dev", select=select, exclude=exclude)

    assert [str(node.addresses.physical) for node in output.compiled_nodes] == expected_physical


class TestParseSelector:
    def test_plain_glob(self):
        assert parse_selector("mydb.analytics.*") == ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=None)

    def test_upstream_only(self):
        assert parse_selector("+mydb.analytics.orders") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=0, downstream_depth=None)

    def test_downstream_only(self):
        assert parse_selector("mydb.analytics.orders+") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=None, downstream_depth=0)

    def test_both_directions(self):
        assert parse_selector("+mydb.analytics.orders+") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=0, downstream_depth=0)

    def test_bounded_upstream(self):
        assert parse_selector("2+mydb.analytics.orders") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=2, downstream_depth=None)

    def test_bounded_downstream(self):
        assert parse_selector("mydb.analytics.orders+3") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=None, downstream_depth=3)

    def test_bounded_both(self):
        assert parse_selector("2+mydb.analytics.orders+3") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=2, downstream_depth=3)

    def test_wildcard_with_upstream(self):
        assert parse_selector("+mydb.analytics.*") == ParsedSelector(glob="mydb.analytics.*", upstream_depth=0, downstream_depth=None)

    def test_wildcard_with_downstream(self):
        assert parse_selector("mydb.analytics.*+") == ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=0)

    def test_upstream_zero_is_explicit_unlimited(self):
        # The text 0+ gives a zero, and thus it is equal to + and has no limit.
        assert parse_selector("0+mydb.analytics.orders") == ParsedSelector(glob="mydb.analytics.orders", upstream_depth=0, downstream_depth=None)


# The graph that the operator tests use:
#
#   db.raw.source_a ──┬──► db.analytics.orders ──► db.analytics.revenue
#   db.raw.source_b ──┘
#

@pytest.fixture
def dag():
    return build_dag_of(
        [
            ("db.raw.source_a", TrouveType.SOURCE),
            ("db.raw.source_b", TrouveType.SOURCE),
            ("db.analytics.orders", TrouveType.TABLE),
            ("db.analytics.revenue", TrouveType.TABLE),
        ],
        [
            ("db.raw.source_a", "db.analytics.orders"),
            ("db.raw.source_b", "db.analytics.orders"),
            ("db.analytics.orders", "db.analytics.revenue"),
        ],
    )


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("db.analytics.orders", {"db.analytics.orders"}),
        ("db.analytics.*", {"db.analytics.orders", "db.analytics.revenue"}),
        ("db.*.orders", {"db.analytics.orders"}),
        ("*.*.*", {"db.raw.source_a", "db.raw.source_b", "db.analytics.orders", "db.analytics.revenue"}),
        ("otherdb.*.*", set()),
    ],
)
def test_a_glob_matches_the_logical_address(dag, pattern, expected):
    assert expand_selector(dag, pattern) == expected


class TestExpandSelector:
    def test_plain_glob_no_traversal(self, dag):
        result = expand_selector(dag, "db.analytics.orders")
        assert result == {"db.analytics.orders"}

    def test_plain_wildcard(self, dag):
        result = expand_selector(dag, "db.analytics.*")
        assert result == {"db.analytics.orders", "db.analytics.revenue"}

    def test_upstream_unlimited(self, dag):
        result = expand_selector(dag, "+db.analytics.orders")
        assert result == {"db.raw.source_a", "db.raw.source_b", "db.analytics.orders"}

    def test_downstream_unlimited(self, dag):
        result = expand_selector(dag, "db.analytics.orders+")
        assert result == {"db.analytics.orders", "db.analytics.revenue"}

    def test_both_directions(self, dag):
        result = expand_selector(dag, "+db.analytics.orders+")
        assert result == {
            "db.raw.source_a",
            "db.raw.source_b",
            "db.analytics.orders",
            "db.analytics.revenue",
        }

    def test_upstream_depth_1(self, dag):
        result = expand_selector(dag, "1+db.analytics.orders")
        assert result == {"db.raw.source_a", "db.raw.source_b", "db.analytics.orders"}

    def test_upstream_depth_1_on_revenue_stops_at_orders(self, dag):
        # The parent of revenue is orders. A depth of 1 omits source_a and source_b.
        result = expand_selector(dag, "1+db.analytics.revenue")
        assert result == {"db.analytics.orders", "db.analytics.revenue"}

    def test_upstream_depth_2_on_revenue_includes_sources(self, dag):
        result = expand_selector(dag, "2+db.analytics.revenue")
        assert result == {
            "db.raw.source_a",
            "db.raw.source_b",
            "db.analytics.orders",
            "db.analytics.revenue",
        }

    def test_downstream_depth_1(self, dag):
        result = expand_selector(dag, "db.raw.source_a+1")
        assert result == {"db.raw.source_a", "db.analytics.orders"}

    def test_downstream_depth_2(self, dag):
        result = expand_selector(dag, "db.raw.source_a+2")
        assert result == {"db.raw.source_a", "db.analytics.orders", "db.analytics.revenue"}

    def test_no_match_returns_empty(self, dag):
        result = expand_selector(dag, "+db.nope.missing")
        assert result == set()

    def test_root_node_has_no_upstream(self, dag):
        result = expand_selector(dag, "+db.raw.source_a")
        assert result == {"db.raw.source_a"}

    def test_leaf_node_has_no_downstream(self, dag):
        result = expand_selector(dag, "db.analytics.revenue+")
        assert result == {"db.analytics.revenue"}


class TestExpandSelectors:
    def test_none_returns_all_in_topo_order(self, dag):
        result = expand_selectors(dag, None)
        # The four nodes. Each source comes before the node that reads it.
        assert set(result) == {
            "db.raw.source_a",
            "db.raw.source_b",
            "db.analytics.orders",
            "db.analytics.revenue",
        }
        assert result.index("db.analytics.orders") < result.index("db.analytics.revenue")

    def test_empty_tuple_returns_all(self, dag):
        result = expand_selectors(dag, ())
        assert len(result) == 4

    def test_single_pattern(self, dag):
        result = expand_selectors(dag, ("db.analytics.orders",))
        assert result == ["db.analytics.orders"]

    def test_multiple_patterns_union(self, dag):
        result = expand_selectors(dag, ("db.raw.source_a", "db.analytics.revenue"))
        assert set(result) == {"db.raw.source_a", "db.analytics.revenue"}

    def test_result_is_topologically_ordered(self, dag):
        result = expand_selectors(dag, ("+db.analytics.revenue",))
        assert set(result) == {
            "db.raw.source_a",
            "db.raw.source_b",
            "db.analytics.orders",
            "db.analytics.revenue",
        }
        assert result.index("db.analytics.orders") < result.index("db.analytics.revenue")
        assert result.index("db.raw.source_a") < result.index("db.analytics.orders")

    def test_union_deduplicates(self, dag):
        # The two patterns overlap. The result holds orders one time only.
        result = expand_selectors(dag, ("db.analytics.*", "db.analytics.orders+"))
        assert result.count("db.analytics.orders") == 1
        assert result.count("db.analytics.revenue") == 1
