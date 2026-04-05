"""Routing policies -- remap logical (database, schema, table) triples to physical targets."""

from __future__ import annotations

import re
from abc import abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from clair.exceptions import InvalidRoutingConfigError
from clair.trouves.trouve import TrouveType


_VALID_IDENTIFIER = re.compile(r"^[A-Z0-9_]+$")


class RoutingConfig(BaseModel):
    """Base class for all routing policies."""

    policy: str

    @abstractmethod
    def apply(self, logical_name: str) -> str:
        """Remap a logical full_name to its physical target.

        Args:
            logical_name: Filesystem-derived "database.schema.table" name.

        Returns:
            The routed full_name string.

        Raises:
            InvalidRoutingConfigError: If the routed identifier is invalid.
        """


class DatabaseOverrideRouting(RoutingConfig):
    """Replace the database component of every non-SOURCE Trouve's full_name."""

    policy: Literal["database_override"] = "database_override"
    database_name: str

    def apply(self, logical_name: str) -> str:
        _, schema, table = logical_name.split(".")
        return f"{self.database_name}.{schema}.{table}"


class SchemaIsolationRouting(RoutingConfig):
    """Collapse database.schema.table into a single table token under a fixed database and schema."""

    policy: Literal["schema_isolation"] = "schema_isolation"
    database_name: str
    schema_name: str

    def apply(self, logical_name: str) -> str:
        db, schema, table = logical_name.split(".")
        new_table = f"{db}_{schema}_{table}".upper()
        if not _VALID_IDENTIFIER.match(new_table):
            raise InvalidRoutingConfigError(
                f"schema_isolation produced invalid identifier '{new_table}' "
                "(only A-Z, 0-9, _ are allowed)"
            )
        if len(new_table) > 255:
            raise InvalidRoutingConfigError(
                f"schema_isolation produced identifier '{new_table}' "
                f"({len(new_table)} chars, max 255)"
            )
        return f"{self.database_name}.{self.schema_name}.{new_table}"


# Discriminated union used for parsing routing blocks from YAML/dicts.
Routing = Annotated[
    DatabaseOverrideRouting | SchemaIsolationRouting,
    Field(discriminator="policy"),
]


def route(
    logical_name: str,
    trouve_type: TrouveType,
    routing: RoutingConfig | None,
) -> str:
    """Apply a routing policy to a logical full_name.

    SOURCE Trouves always pass through regardless of the routing policy.

    Args:
        logical_name: Filesystem-derived "database.schema.table" name.
        trouve_type: SOURCE, TABLE, or VIEW.
        routing: Active routing config, or None for passthrough.

    Returns:
        The routed full_name string.
    """
    if routing is None or trouve_type == TrouveType.SOURCE:
        return logical_name
    return routing.apply(logical_name)


def detect_routing_collisions(logical_to_routed: dict[str, str]) -> list[tuple[str, list[str]]]:
    """Return (target, sources) pairs for any routing collisions.

    A collision occurs when two TABLE/VIEW Trouves route to the same physical target.
    The last write in execution order will determine the final state of that target.

    Args:
        logical_to_routed: Mapping of logical_name -> routed_name for non-SOURCE Trouves.

    Returns:
        List of (routed_target, [logical_source, ...]) for each collision found.
    """
    target_to_sources: dict[str, list[str]] = {}
    for logical, routed in logical_to_routed.items():
        target_to_sources.setdefault(routed, []).append(logical)

    return [
        (target, sorted(sources))
        for target, sources in target_to_sources.items()
        if len(sources) > 1
    ]
