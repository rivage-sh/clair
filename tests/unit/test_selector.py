"""Tests for the selector/glob matching."""

from clair.core.selector import filter_by_selector, filter_by_selectors, match_selector


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
