"""Tests for routing policies."""

from __future__ import annotations

import pytest

from pydantic import ValidationError

from clair.environments.routing import (
    DatabaseOverrideRouting,
    SchemaIsolationRouting,
    detect_routing_collisions,
    route,
)
from clair.exceptions import InvalidRoutingConfigError
from clair.trouves.trouve import TrouveType


def _db_override(database_name: str) -> DatabaseOverrideRouting:
    return DatabaseOverrideRouting(database_name=database_name)


def _schema_isolation(database_name: str, schema_name: str) -> SchemaIsolationRouting:
    return SchemaIsolationRouting(database_name=database_name, schema_name=schema_name)


class TestRoute:
    def test_passthrough_when_no_routing(self):
        result = route("analytics.finance.revenue", TrouveType.TABLE, None)
        assert result == "analytics.finance.revenue"

    def test_source_passthrough_with_routing(self):
        routing = _db_override("OMER_DEV")
        result = route("analytics.finance.revenue", TrouveType.SOURCE, routing)
        assert result == "analytics.finance.revenue"

    def test_source_passthrough_with_schema_isolation(self):
        routing = _schema_isolation("DEV", "obaddour")
        result = route("refined.products.catalog", TrouveType.SOURCE, routing)
        assert result == "refined.products.catalog"

    def test_database_override_table(self):
        routing = _db_override("OMER_DEV")
        result = route("analytics.finance.revenue", TrouveType.TABLE, routing)
        assert result == "OMER_DEV.finance.revenue"

    def test_database_override_view(self):
        routing = _db_override("OMER_DEV")
        result = route("analytics.finance.revenue", TrouveType.VIEW, routing)
        assert result == "OMER_DEV.finance.revenue"

    def test_database_override_preserves_schema_and_table(self):
        routing = _db_override("MY_DEV_DB")
        result = route("warehouse.orders.daily", TrouveType.TABLE, routing)
        assert result == "MY_DEV_DB.orders.daily"

    def test_schema_isolation_table(self):
        routing = _schema_isolation("DEV", "obaddour")
        result = route("refined.products.catalog", TrouveType.TABLE, routing)
        assert result == "DEV.obaddour.REFINED_PRODUCTS_CATALOG"

    def test_schema_isolation_concatenates_all_three_parts(self):
        routing = _schema_isolation("DEV", "myschema")
        result = route("analytics.finance.revenue", TrouveType.TABLE, routing)
        assert result == "DEV.myschema.ANALYTICS_FINANCE_REVENUE"

    def test_schema_isolation_identifier_exceeds_255_chars_raises(self):
        routing = _schema_isolation("DEV", "myschema")
        long_table = "a" * 250
        with pytest.raises(InvalidRoutingConfigError, match="255"):
            route(f"db.schema.{long_table}", TrouveType.TABLE, routing)

    def test_no_routing_passthrough_for_view(self):
        result = route("analytics.finance.summary", TrouveType.VIEW, None)
        assert result == "analytics.finance.summary"


class TestDetectRoutingCollisions:
    def test_no_collision_returns_empty(self):
        result = detect_routing_collisions({
            "analytics.finance.revenue": "OMER_DEV.finance.revenue",
            "warehouse.orders.daily": "OMER_DEV.orders.daily",
        })
        assert result == []

    def test_collision_returns_target_and_sources(self):
        result = detect_routing_collisions({
            "analytics.finance.orders": "OMER_DEV.finance.orders",
            "warehouse.finance.orders": "OMER_DEV.finance.orders",
        })
        assert len(result) == 1
        target, sources = result[0]
        assert target == "OMER_DEV.finance.orders"
        assert sorted(sources) == ["analytics.finance.orders", "warehouse.finance.orders"]

    def test_empty_dict_returns_empty(self):
        assert detect_routing_collisions({}) == []

    def test_single_entry_returns_empty(self):
        assert detect_routing_collisions({"analytics.finance.revenue": "OMER_DEV.finance.revenue"}) == []


class TestRoutingConfigValidation:
    def test_schema_isolation_requires_schema_name(self):
        with pytest.raises(ValidationError):
            SchemaIsolationRouting.model_validate({"database_name": "DEV"})

    def test_database_override_does_not_require_schema_name(self):
        config = DatabaseOverrideRouting(database_name="OMER_DEV")
        assert not hasattr(config, "schema_name")

    def test_schema_name_field(self):
        config = SchemaIsolationRouting(database_name="DEV", schema_name="myschema")
        assert config.schema_name == "myschema"
