"""Routing -- remaps the logical address of a Trouve to a physical address.

Three types make up the routing system:

* ``TrouveAddress`` holds a database name, a schema name, and a table name. It
  validates each name when you make it. An address that exists is a valid one.
* ``RoutingEntry`` is the base class for one environment's rule. A user writes a
  subclass and gives it a ``route`` method.
* ``RoutingTable`` holds the entries. The project ``__routing__.py`` makes one.

The validation happens in ``TrouveAddress``, not after a rule runs. A rule that
gives an address gives a correct address, or it raises an error.

At this time ``TrouveAddress`` applies the Snowflake identifier rules.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)

from clair.exceptions import InvalidRoutingConfigError, InvalidTrouveAddressError
from clair.trouves.trouve import TrouveType

if TYPE_CHECKING:
    from clair.trouves.trouve import TrouveAbc


# Snowflake accepts these characters in an unquoted identifier.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_MAX_IDENTIFIER_LENGTH = 255

# Maximum width of an entry description in a CLI message.
_MAX_DESCRIPTION_LENGTH = 200


def _first_error_message(exc: ValidationError) -> str:
    """Take the first message out of a Pydantic error.

    Pydantic prints a report of many lines. A CLI message needs one sentence.
    """
    errors = exc.errors()
    if not errors:
        return str(exc)
    first = errors[0]
    message = str(first.get("msg", "")).removeprefix("Value error, ")
    location = ".".join(str(part) for part in first.get("loc", ()))
    return f"{location}: {message}" if location else message


class TrouveAddress(BaseModel):
    """The full address of one Trouve in the warehouse.

    The model is frozen, so an address is hashable and safe to share. To make a
    changed copy, call ``model_copy(update={...})``.
    """

    model_config = ConfigDict(frozen=True)

    database_name: str
    schema_name: str
    table_name: str

    @field_validator("database_name", "schema_name", "table_name")
    @classmethod
    def _validate_identifier(cls, value: str) -> str:
        """Reject a name that Snowflake cannot use as an unquoted identifier."""
        if len(value) > _MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                f"'{value}' has {len(value)} characters. "
                f"The maximum is {_MAX_IDENTIFIER_LENGTH}."
            )
        if not _VALID_IDENTIFIER.match(value):
            raise ValueError(
                f"'{value}' is not a valid identifier. An identifier starts with a "
                "letter or an underscore. The other characters are letters, digits, "
                "underscores or dollar signs."
            )
        return value

    @classmethod
    def parse(cls, full_name: str) -> TrouveAddress:
        """Make an address from a "database_name.schema_name.table_name" string.

        Args:
            full_name: The dotted name.

        Returns:
            The validated address.

        Raises:
            InvalidTrouveAddressError: If the string is not a valid address.
        """
        parts = full_name.split(".")
        if len(parts) != 3:
            raise InvalidTrouveAddressError(
                full_name,
                f"an address needs 3 dot-separated parts, but this name has "
                f"{len(parts)}",
            )
        try:
            return cls(
                database_name=parts[0], schema_name=parts[1], table_name=parts[2]
            )
        except ValidationError as exc:
            raise InvalidTrouveAddressError(
                full_name, _first_error_message(exc)
            ) from exc

    def __str__(self) -> str:
        return f"{self.database_name}.{self.schema_name}.{self.table_name}"


class RoutingEntry(BaseModel, ABC):
    """The routing rule for one environment.

    Write a subclass and give it a ``route`` method. Add a field for each value
    that the rule needs. Pydantic validates the fields, and the field values
    show in the CLI messages.

    Example::

        class DeveloperRouting(RoutingEntry):
            environment_name: str = "dev"
            user_variable: str = "CLAIR_USER"

            def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
                user = os.environ[self.user_variable].upper()
                return trouve_address.model_copy(
                    update={"database_name": f"{trouve_address.database_name}_{user}"}
                )
    """

    # The join key. It matches a top-level key in ~/.clair/environments.yml.
    environment_name: str

    @abstractmethod
    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        """Give the physical address for one logical address.

        Clair never calls this method for a SOURCE Trouve. A SOURCE always reads
        from the address that the file system gives.

        Args:
            trouve_address: The logical address of the Trouve.

        Returns:
            The physical address to write to.
        """


class RoutingTable(BaseModel):
    """All the routing entries for one project.

    The project ``__routing__.py`` makes one table and gives it the name
    ``routing``. Two entries for one environment name are an error.
    """

    entries: list[RoutingEntry] = []

    @model_validator(mode="after")
    def _reject_duplicate_environment_names(self) -> RoutingTable:
        """Stop a second entry for one environment name.

        Only one entry can win. A silent choice between two entries would send
        the writes to a target that the user does not expect.
        """
        seen: set[str] = set()
        duplicates: set[str] = set()
        for entry in self.entries:
            if entry.environment_name in seen:
                duplicates.add(entry.environment_name)
            seen.add(entry.environment_name)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(
                f"the routing table has more than one entry for: {names}. "
                "Give each environment one entry."
            )
        return self

    @property
    def environment_names(self) -> list[str]:
        """Give the sorted names of every environment in the table."""
        return sorted(entry.environment_name for entry in self.entries)

    def entry_for(self, environment_name: str) -> RoutingEntry | None:
        """Find the entry for one environment name, or None."""
        for entry in self.entries:
            if entry.environment_name == environment_name:
                return entry
        return None


def describe_routing(routing: RoutingEntry | None) -> str:
    """Give a short description of a routing entry for a CLI message.

    Pydantic prints the class name and the field values, which tells the reader
    why two Trouves went to one target.
    """
    if routing is None:
        return "none"
    description = repr(routing)
    if len(description) > _MAX_DESCRIPTION_LENGTH:
        return description[: _MAX_DESCRIPTION_LENGTH - 1] + "…"
    return description


def _apply_routing(
    logical_address: TrouveAddress, routing: RoutingEntry
) -> TrouveAddress:
    """Run one routing entry and confirm that it gave an address.

    The entry builds a ``TrouveAddress``, so the address rules apply already.
    This function adds the context that the address alone does not hold: which
    entry ran, and which Trouve it ran on.
    """
    entry_text = f"The routing entry `{describe_routing(routing)}`"
    try:
        physical_address = routing.route(logical_address)
    except ValidationError as exc:
        raise InvalidRoutingConfigError(
            f"{entry_text} built a bad address for '{logical_address}': "
            f"{_first_error_message(exc)}"
        ) from exc
    except (InvalidRoutingConfigError, InvalidTrouveAddressError):
        raise
    except Exception as exc:
        raise InvalidRoutingConfigError(
            f"{entry_text} failed on '{logical_address}': {type(exc).__name__}: {exc}"
        ) from exc

    if not isinstance(physical_address, TrouveAddress):
        raise InvalidRoutingConfigError(
            f"{entry_text} gave {type(physical_address).__name__} for "
            f"'{logical_address}'. A route method must give a TrouveAddress."
        )
    return physical_address


def route(
    logical_name: str,
    trouve_type: TrouveType,
    routing: RoutingEntry | None,
) -> str:
    """Apply a routing entry to a logical full_name.

    The function validates the logical name first, then applies the entry. A
    SOURCE Trouve keeps its logical name, whatever the entry is.

    Args:
        logical_name: The file system name "database_name.schema_name.table_name".
        trouve_type: SOURCE, TABLE, or VIEW.
        routing: The active routing entry, or None for passthrough.

    Returns:
        The physical full_name string.

    Raises:
        InvalidTrouveAddressError: If the logical name is not a valid address.
        InvalidRoutingConfigError: If the entry fails, or gives a bad address.
    """
    logical_address = TrouveAddress.parse(logical_name)

    if routing is None or trouve_type == TrouveType.SOURCE:
        return str(logical_address)

    return str(_apply_routing(logical_address, routing))


def collect_routing_problems(
    trouves: list[TrouveAbc],
    routing: RoutingEntry | None,
) -> list[tuple[str, str]]:
    """Apply a routing entry to every Trouve and collect all the failures.

    ``route()`` stops at the first bad address, which is correct for a run. This
    function instead reports every problem at once, so that ``clair validate``
    shows a complete list.

    Args:
        trouves: All the Trouves in the project, found with routing off.
        routing: The routing entry to test.

    Returns:
        A list of (logical_name, problem_text) pairs, in discovery order.
    """
    problems: list[tuple[str, str]] = []
    for trouve in trouves:
        if not trouve.compiled:
            continue
        logical_name = trouve.compiled.logical_name
        try:
            route(logical_name, trouve.type, routing)
        except (InvalidRoutingConfigError, InvalidTrouveAddressError) as exc:
            problems.append((logical_name, str(exc)))
    return problems


def detect_routing_collisions(
    logical_to_routed: dict[str, str],
) -> list[tuple[str, list[str]]]:
    """Give the (target, sources) pairs for the routing collisions.

    A collision happens when two TABLE or VIEW Trouves go to one physical target.
    The last write in execution order sets the final state of that target.

    Args:
        logical_to_routed: A map of logical_name to routed_name for the Trouves
            that are not SOURCE Trouves.

    Returns:
        A list of (routed_target, [logical_source, ...]) for each collision.
    """
    target_to_sources: dict[str, list[str]] = {}
    for logical, routed in logical_to_routed.items():
        target_to_sources.setdefault(routed, []).append(logical)

    return [
        (target, sorted(sources))
        for target, sources in target_to_sources.items()
        if len(sources) > 1
    ]
