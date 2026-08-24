"""Tests for TrouveAddress, RoutingEntry, RoutingTable, and route()."""

from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from clair.environments.routing import (
    RoutingEntry,
    RoutingTable,
    TrouveAddress,
    collect_routing_problems,
    describe_routing,
    detect_routing_collisions,
    route,
)
from clair.exceptions import InvalidRoutingConfigError, InvalidTrouveAddressError
from clair.trouves.trouve import TrouveType
from tests.helpers import DatabaseOverrideRouting, SchemaIsolationRouting


class TestTrouveAddress:
    def test_parse_splits_the_three_names(self):
        address = TrouveAddress.parse("analytics.finance.revenue")
        assert address.database_name == "analytics"
        assert address.schema_name == "finance"
        assert address.table_name == "revenue"

    def test_str_joins_the_three_names(self):
        address = TrouveAddress.parse("analytics.finance.revenue")
        assert str(address) == "analytics.finance.revenue"

    def test_address_is_frozen(self):
        address = TrouveAddress.parse("a.b.c")
        with pytest.raises(ValidationError):
            setattr(address, "database_name", "other")  # noqa: B010

    def test_address_is_hashable(self):
        assert len({TrouveAddress.parse("a.b.c"), TrouveAddress.parse("a.b.c")}) == 1

    def test_two_parts_raise(self):
        with pytest.raises(InvalidTrouveAddressError, match="3 dot-separated parts"):
            TrouveAddress.parse("finance.revenue")

    def test_four_parts_raise(self):
        with pytest.raises(InvalidTrouveAddressError, match="3 dot-separated parts"):
            TrouveAddress.parse("a.b.c.d")

    @pytest.mark.parametrize(
        "bad_name",
        ["my-db.b.c", "a.my-schema.c", "a.b.my-table", "1db.b.c", "a..c"],
    )
    def test_invalid_identifier_raises(self, bad_name: str):
        with pytest.raises(InvalidTrouveAddressError, match="not a valid identifier"):
            TrouveAddress.parse(bad_name)

    def test_the_message_names_the_bad_part(self):
        with pytest.raises(InvalidTrouveAddressError, match="schema_name"):
            TrouveAddress.parse("a.bad-schema.c")

    def test_identifier_over_255_characters_raises(self):
        with pytest.raises(InvalidTrouveAddressError, match="255"):
            TrouveAddress.parse(f"db.schema.{'a' * 256}")

    def test_dollar_sign_and_underscore_are_valid(self):
        assert str(TrouveAddress.parse("_db.s$1.T_2")) == "_db.s$1.T_2"

    def test_direct_construction_validates(self):
        with pytest.raises(ValidationError):
            TrouveAddress(database_name="my-db", schema_name="b", table_name="c")


class TestRoutingEntry:
    def test_the_base_class_needs_a_route_method(self):
        with pytest.raises(TypeError):
            RoutingEntry(environment_name="dev")

    def test_a_subclass_keeps_its_own_fields(self):
        entry = DatabaseOverrideRouting(
            environment_name="dev", database_name="OMER_DEV"
        )
        assert entry.database_name == "OMER_DEV"

    def test_pydantic_validates_a_subclass_field(self):
        """An absent database_name is an error, not a silent default."""
        with pytest.raises(ValidationError):
            DatabaseOverrideRouting.model_validate({"environment_name": "dev"})


class TestRoutingTable:
    def test_entry_for_finds_the_named_entry(self):
        entry = DatabaseOverrideRouting(environment_name="dev", database_name="DEV")
        table = RoutingTable(entries=[entry])
        assert table.entry_for("dev") is entry

    def test_entry_for_gives_none_for_an_absent_name(self):
        assert RoutingTable(entries=[]).entry_for("dev") is None

    def test_a_subclass_survives_the_table(self):
        """Pydantic must not reduce an entry to its RoutingEntry base class."""
        table = RoutingTable(
            entries=[SchemaIsolationRouting(database_name="DEV", schema_name="mine")]
        )
        entry = table.entry_for("dev")
        assert isinstance(entry, SchemaIsolationRouting)
        assert entry.schema_name == "mine"

    def test_environment_names_are_sorted(self):
        table = RoutingTable(entries=[
            DatabaseOverrideRouting(environment_name="prod", database_name="P"),
            DatabaseOverrideRouting(environment_name="dev", database_name="D"),
        ])
        assert table.environment_names == ["dev", "prod"]

    def test_duplicate_environment_name_raises(self):
        with pytest.raises(ValidationError, match="more than one entry"):
            RoutingTable(entries=[
                DatabaseOverrideRouting(environment_name="dev", database_name="A"),
                DatabaseOverrideRouting(environment_name="dev", database_name="B"),
            ])

    def test_the_duplicate_message_names_the_environment(self):
        with pytest.raises(ValidationError, match="staging"):
            RoutingTable(entries=[
                DatabaseOverrideRouting(environment_name="staging", database_name="A"),
                DatabaseOverrideRouting(environment_name="staging", database_name="B"),
            ])

    def test_an_empty_table_is_valid(self):
        assert RoutingTable().environment_names == []


def _db_override(database_name: str) -> DatabaseOverrideRouting:
    return DatabaseOverrideRouting(database_name=database_name)


def _schema_isolation(database_name: str, schema_name: str) -> SchemaIsolationRouting:
    return SchemaIsolationRouting(database_name=database_name, schema_name=schema_name)


class TestRoute:
    def test_passthrough_when_no_routing(self):
        assert route("analytics.finance.revenue", TrouveType.TABLE, None) == (
            "analytics.finance.revenue"
        )

    def test_source_passthrough_with_routing(self):
        result = route(
            "analytics.finance.revenue", TrouveType.SOURCE, _db_override("OMER_DEV")
        )
        assert result == "analytics.finance.revenue"

    def test_database_override_table(self):
        result = route(
            "analytics.finance.revenue", TrouveType.TABLE, _db_override("OMER_DEV")
        )
        assert result == "OMER_DEV.finance.revenue"

    def test_database_override_view(self):
        result = route(
            "analytics.finance.revenue", TrouveType.VIEW, _db_override("OMER_DEV")
        )
        assert result == "OMER_DEV.finance.revenue"

    def test_database_override_keeps_the_schema_and_the_table(self):
        result = route(
            "warehouse.orders.daily", TrouveType.TABLE, _db_override("MY_DEV_DB")
        )
        assert result == "MY_DEV_DB.orders.daily"

    def test_schema_isolation_table(self):
        result = route(
            "refined.products.catalog",
            TrouveType.TABLE,
            _schema_isolation("DEV", "obaddour"),
        )
        assert result == "DEV.obaddour.REFINED_PRODUCTS_CATALOG"

    def test_route_validates_the_logical_name(self):
        """A file system name that Snowflake cannot use is an error, with no entry."""
        with pytest.raises(InvalidTrouveAddressError, match="not a valid identifier"):
            route("my-db.finance.revenue", TrouveType.TABLE, None)

    def test_route_validates_the_logical_name_of_a_source(self):
        with pytest.raises(InvalidTrouveAddressError):
            route("my-db.finance.revenue", TrouveType.SOURCE, None)

    def test_an_entry_that_makes_a_long_name_raises(self):
        routing = _schema_isolation("DEV", "myschema")
        with pytest.raises(InvalidRoutingConfigError, match="255"):
            route(f"db.schema.{'a' * 250}", TrouveType.TABLE, routing)

    def test_the_error_names_the_entry_and_the_trouve(self):
        routing = _schema_isolation("DEV", "myschema")
        with pytest.raises(InvalidRoutingConfigError) as exc_info:
            route(f"db.schema.{'a' * 250}", TrouveType.TABLE, routing)
        message = str(exc_info.value)
        assert "SchemaIsolationRouting" in message
        assert "db.schema." in message


class TestRouteRejectsABadEntry:
    def test_an_entry_that_gives_a_string_raises(self):
        class StringRouting(RoutingEntry):
            environment_name: str = "dev"

            def route(self, trouve_address):
                return "a.b.c"

        with pytest.raises(
            InvalidRoutingConfigError, match="must give a TrouveAddress"
        ):
            route("analytics.finance.revenue", TrouveType.TABLE, StringRouting())

    def test_an_entry_that_gives_none_raises(self):
        class NoneRouting(RoutingEntry):
            environment_name: str = "dev"

            def route(self, trouve_address):
                return None

        with pytest.raises(InvalidRoutingConfigError, match="NoneType"):
            route("analytics.finance.revenue", TrouveType.TABLE, NoneRouting())

    def test_an_entry_that_raises_is_wrapped(self, monkeypatch):
        monkeypatch.delenv("CLAIR_USER", raising=False)

        class UserRouting(RoutingEntry):
            environment_name: str = "dev"

            def route(self, trouve_address):
                user = os.environ["CLAIR_USER"]
                return trouve_address.model_copy(
                    update={"database_name": f"{trouve_address.database_name}_{user}"}
                )

        with pytest.raises(InvalidRoutingConfigError, match="KeyError"):
            route("analytics.finance.revenue", TrouveType.TABLE, UserRouting())

    def test_an_entry_that_builds_a_bad_address_raises(self):
        class DashRouting(RoutingEntry):
            environment_name: str = "dev"

            def route(self, trouve_address):
                return TrouveAddress(
                    database_name="my-db",
                    schema_name=trouve_address.schema_name,
                    table_name=trouve_address.table_name,
                )

        with pytest.raises(InvalidRoutingConfigError, match="not a valid identifier"):
            route("analytics.finance.revenue", TrouveType.TABLE, DashRouting())

    def test_an_entry_reads_an_environment_variable(self, monkeypatch):
        monkeypatch.setenv("CLAIR_USER", "obaddour")

        class UserRouting(RoutingEntry):
            environment_name: str = "dev"

            def route(self, trouve_address):
                user = os.environ["CLAIR_USER"].upper()
                return trouve_address.model_copy(
                    update={"database_name": f"{trouve_address.database_name}_{user}"}
                )

        result = route("analytics.finance.revenue", TrouveType.TABLE, UserRouting())
        assert result == "analytics_OBADDOUR.finance.revenue"


class TestDescribeRouting:
    def test_describes_none(self):
        assert describe_routing(None) == "none"

    def test_names_the_class_and_the_fields(self):
        description = describe_routing(_db_override("OMER_DEV"))
        assert "DatabaseOverrideRouting" in description
        assert "OMER_DEV" in description

    def test_names_a_second_field(self):
        description = describe_routing(_schema_isolation("DEV", "obaddour"))
        assert "DEV" in description
        assert "obaddour" in description

    def test_the_description_stays_on_one_line(self):
        assert "\n" not in describe_routing(_db_override("OMER_DEV"))

    def test_a_long_description_is_cut_short(self):
        description = describe_routing(_db_override("A" * 300))
        assert len(description) == 200
        assert description.endswith("…")


class TestLogicalNameValidation:
    """A directory name that Snowflake cannot use stops every command."""

    def test_a_bad_directory_name_stops_discovery(self, tmp_path):
        from clair.core.discovery import discover_project

        (tmp_path / "my-db" / "finance").mkdir(parents=True)
        (tmp_path / "my-db" / "finance" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            'trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS x")\n'
        )
        with pytest.raises(InvalidTrouveAddressError, match="my-db"):
            discover_project(tmp_path, routing=None)

    def test_a_good_directory_name_passes_discovery(self, tmp_path):
        from clair.core.discovery import discover_project

        (tmp_path / "my_db" / "finance").mkdir(parents=True)
        (tmp_path / "my_db" / "finance" / "revenue.py").write_text(
            "from clair import Trouve, TrouveType\n\n"
            'trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS x")\n'
        )
        trouves = discover_project(tmp_path, routing=None)
        assert collect_routing_problems(trouves, None) == []


class TestDetectRoutingCollisions:
    def test_no_collision_returns_empty(self):
        assert detect_routing_collisions({
            "analytics.finance.revenue": "OMER_DEV.finance.revenue",
            "warehouse.orders.daily": "OMER_DEV.orders.daily",
        }) == []

    def test_collision_gives_the_target_and_the_sources(self):
        result = detect_routing_collisions({
            "analytics.finance.orders": "OMER_DEV.finance.orders",
            "warehouse.finance.orders": "OMER_DEV.finance.orders",
        })
        assert len(result) == 1
        target, sources = result[0]
        assert target == "OMER_DEV.finance.orders"
        assert sorted(sources) == [
            "analytics.finance.orders",
            "warehouse.finance.orders",
        ]

    def test_empty_dict_returns_empty(self):
        assert detect_routing_collisions({}) == []

    def test_single_entry_returns_empty(self):
        assert detect_routing_collisions(
            {"analytics.finance.revenue": "OMER_DEV.finance.revenue"}
        ) == []
