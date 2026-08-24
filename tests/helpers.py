"""Routing entries that the tests share.

Clair ships no concrete ``RoutingEntry``. A user writes one. These entries give
the tests the shapes that a user writes most often.
"""

from __future__ import annotations

from clair.environments.routing import RoutingEntry, TrouveAddress
from clair.trouves.trouve import TrouveType


class DatabaseOverrideRouting(RoutingEntry):
    """Send every Trouve, a SOURCE too, to one database."""

    environment_name: str = "dev"
    database_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address.model_copy(update={"database_name": self.database_name})


class SchemaIsolationRouting(RoutingEntry):
    """Collapse the three names into one table name under a fixed schema."""

    environment_name: str = "dev"
    database_name: str
    schema_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        collapsed_table_name = (
            f"{trouve_address.database_name}_{trouve_address.schema_name}_"
            f"{trouve_address.table_name}"
        ).upper()
        return TrouveAddress(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=collapsed_table_name,
        )


class SourceAwareRouting(RoutingEntry):
    """Send each TABLE and VIEW to one database, and keep each SOURCE."""

    environment_name: str = "dev"
    database_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return trouve_address
        return trouve_address.model_copy(update={"database_name": self.database_name})
