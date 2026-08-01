"""The routing policies. Each policy maps a logical name to a physical target."""

from __future__ import annotations

import re
from abc import abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from clair.exceptions import InvalidRoutingConfigError
from clair.trouves.trouve import TrouveType

_VALID_IDENTIFIER = re.compile(r"^[A-Z0-9_]+$")


class RoutingConfig(BaseModel):
    """The parent class of all the routing policies."""

    policy: str

    @abstractmethod
    def apply(self, logical_name: str) -> str:
        """Map a logical full_name to its physical target.

        Args:
            logical_name: The "database.schema.table" name from the file path.

        Returns:
            The routed full_name.

        Raises:
            InvalidRoutingConfigError: If the routed identifier is not correct.
        """


class DatabaseOverrideRouting(RoutingConfig):
    """Replace the database part of the full_name of each Trouve that is not a SOURCE."""

    policy: Literal["database_override"] = "database_override"
    database_name: str

    def apply(self, logical_name: str) -> str:
        _, schema, table = logical_name.split(".")
        return f"{self.database_name}.{schema}.{table}"


class SchemaIsolationRouting(RoutingConfig):
    """Join database.schema.table into one table name in a constant database and schema."""

    policy: Literal["schema_isolation"] = "schema_isolation"
    database_name: str
    schema_name: str

    def apply(self, logical_name: str) -> str:
        db, schema, table = logical_name.split(".")
        new_table = f"{db}_{schema}_{table}".upper()
        if not _VALID_IDENTIFIER.match(new_table):
            raise InvalidRoutingConfigError(
                f"schema_isolation made the incorrect identifier '{new_table}'. "
                "Use only the characters A-Z, 0-9 and _."
            )
        if len(new_table) > 255:
            raise InvalidRoutingConfigError(
                f"schema_isolation made the identifier '{new_table}'. "
                f"It has {len(new_table)} characters, but the maximum is 255."
            )
        return f"{self.database_name}.{self.schema_name}.{new_table}"


# The tagged union that clair uses to read a routing block from YAML or a dict.
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

    A SOURCE Trouve always keeps its name, whatever the routing policy is.

    Args:
        logical_name: The "database.schema.table" name from the file path.
        trouve_type: SOURCE, TABLE, or VIEW.
        routing: The active routing config. Give None to keep the name.

    Returns:
        The routed full_name.
    """
    if routing is None or trouve_type == TrouveType.SOURCE:
        return logical_name
    return routing.apply(logical_name)


def detect_routing_collisions(logical_to_routed: dict[str, str]) -> list[tuple[str, list[str]]]:
    """Give a (target, sources) pair for each routing collision.

    A collision occurs when two TABLE or VIEW Trouves route to one physical
    target. The last write sets the final content of that target.

    Args:
        logical_to_routed: A map of logical_name to routed_name. It holds each
            Trouve that is not a SOURCE.

    Returns:
        A list of (routed_target, [logical_source, ...]), one item for each
        collision.
    """
    target_to_sources: dict[str, list[str]] = {}
    for logical, routed in logical_to_routed.items():
        target_to_sources.setdefault(routed, []).append(logical)

    return [
        (target, sorted(sources))
        for target, sources in target_to_sources.items()
        if len(sources) > 1
    ]
