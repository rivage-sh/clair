"""Tests for the selector/glob matching and graph traversal operators."""

import networkx as nx
import pytest

from clair.core.selector import (
    expand_selector,
    expand_selectors,
    filter_by_selector,
    filter_by_selectors,
    match_selector,
    parse_selector,
)


class TestMatchSelector:
    def test_exact_match(self):
        assert match_selector("mydb.analytics.orders", "mydb.analytics.orders")

    def test_wildcard_table(self):
        assert match_selector("mydb.analytics.orders", "mydb.analytics.*")

    def test_wildcard_schema(self):
        assert match_selector("mydb.analytics.orders", "mydb.*.orders")

    def test_no_match(self):
        assert not match_selector("mydb.staging.users", "mydb.analytics.*")

    def test_wildcard_all(self):
        assert match_selector("mydb.analytics.orders", "*.*.*")

    def test_double_wildcard_schema_and_table(self):
        assert match_selector("mydb.analytics.orders", "mydb.*.*")


class TestFilterBySelector:
    def test_none_pattern_returns_all(self):
        names = ["a.b.c", "d.e.f"]
        assert filter_by_selector(names, None) == names

    def test_filters_correctly(self):
        names = ["mydb.analytics.orders", "mydb.staging.users", "mydb.analytics.revenue"]
        result = filter_by_selector(names, "mydb.analytics.*")
        assert result == ["mydb.analytics.orders", "mydb.analytics.revenue"]

    def test_empty_result(self):
        names = ["mydb.analytics.orders"]
        result = filter_by_selector(names, "otherdb.*.*")
        assert result == []


class TestFilterBySelectors:
    def test_none_returns_all(self):
        names = ["a.b.c", "d.e.f"]
        assert filter_by_selectors(names, None) == names

    def test_empty_tuple_returns_all(self):
        names = ["a.b.c", "d.e.f"]
        assert filter_by_selectors(names, ()) == names

    def test_single_pattern_matches(self):
        names = ["mydb.analytics.orders", "mydb.staging.users", "mydb.analytics.revenue"]
        result = filter_by_selectors(names, ("mydb.analytics.*",))
        assert result == ["mydb.analytics.orders", "mydb.analytics.revenue"]

    def test_multiple_patterns_union(self):
        names = [
            "mydb.analytics.orders",
            "mydb.staging.users",
            "mydb.analytics.revenue",
            "mydb.reports.daily",
        ]
        result = filter_by_selectors(names, ("mydb.analytics.*", "mydb.reports.*"))
        assert result == ["mydb.analytics.orders", "mydb.analytics.revenue", "mydb.reports.daily"]

    def test_overlapping_patterns_no_duplicates(self):
        names = ["mydb.analytics.orders", "mydb.analytics.revenue"]
        result = filter_by_selectors(names, ("mydb.analytics.*", "mydb.*.orders"))
        assert result == ["mydb.analytics.orders", "mydb.analytics.revenue"]

    def test_preserves_original_order(self):
        names = ["z.z.z", "a.a.a", "m.m.m"]
        result = filter_by_selectors(names, ("m.m.m", "z.z.z"))
        assert result == ["z.z.z", "m.m.m"]

    def test_no_matches_returns_empty(self):
        names = ["mydb.analytics.orders"]
        result = filter_by_selectors(names, ("otherdb.*.*", "nope.*.*"))
        assert result == []


class TestParseSelector:
    def test_plain_glob(self):
        assert parse_selector("mydb.analytics.*") == (None, "mydb.analytics.*", None)

    def test_upstream_only(self):
        assert parse_selector("+mydb.analytics.orders") == (0, "mydb.analytics.orders", None)

    def test_downstream_only(self):
        assert parse_selector("mydb.analytics.orders+") == (None, "mydb.analytics.orders", 0)

    def test_both_directions(self):
        assert parse_selector("+mydb.analytics.orders+") == (0, "mydb.analytics.orders", 0)

    def test_bounded_upstream(self):
        assert parse_selector("2+mydb.analytics.orders") == (2, "mydb.analytics.orders", None)

    def test_bounded_downstream(self):
        assert parse_selector("mydb.analytics.orders+3") == (None, "mydb.analytics.orders", 3)

    def test_bounded_both(self):
        assert parse_selector("2+mydb.analytics.orders+3") == (2, "mydb.analytics.orders", 3)

    def test_wildcard_with_upstream(self):
        assert parse_selector("+mydb.analytics.*") == (0, "mydb.analytics.*", None)

    def test_wildcard_with_downstream(self):
        assert parse_selector("mydb.analytics.*+") == (None, "mydb.analytics.*", 0)

    def test_upstream_zero_is_explicit_unlimited(self):
        # 0+ means explicit zero (same as +, unlimited)
        assert parse_selector("0+mydb.analytics.orders") == (0, "mydb.analytics.orders", None)


# Graph fixture shared by operator tests:
#
#   db.raw.source_a ──┬──► db.analytics.orders ──► db.analytics.revenue
#   db.raw.source_b ──┘
#

@pytest.fixture
def dag():
    g = nx.DiGraph()
    g.add_nodes_from([
        "db.raw.source_a",
        "db.raw.source_b",
        "db.analytics.orders",
        "db.analytics.revenue",
    ])
    g.add_edge("db.raw.source_a", "db.analytics.orders")
    g.add_edge("db.raw.source_b", "db.analytics.orders")
    g.add_edge("db.analytics.orders", "db.analytics.revenue")
    return g


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
        # revenue's parent is orders; depth=1 should NOT include source_a/source_b
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
        # All four nodes, sources before dependents
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
        # Two patterns that overlap; orders should appear once
        result = expand_selectors(dag, ("db.analytics.*", "db.analytics.orders+"))
        assert result.count("db.analytics.orders") == 1
        assert result.count("db.analytics.revenue") == 1
