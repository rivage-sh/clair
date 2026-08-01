"""Routing rules -- remap logical name triples to physical targets.

A routing rule is one of two kinds:

* A ``RoutingConfig`` subclass, such as ``DatabaseOverrideRouting``.
* Any callable with the signature
  ``(database_name, schema_name, table_name) -> "database_name.schema_name.table_name"``.

Both kinds go through ``route()``, which applies the rule and then validates the
result. That validation step is the reason a callable rule is safe: Python cannot
tell you in advance what a callable returns, so clair examines the output.
"""

from __future__ import annotations

import inspect
import re
from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

from clair.exceptions import InvalidRoutingConfigError
from clair.trouves.trouve import TrouveType

if TYPE_CHECKING:
    from clair.trouves.trouve import Trouve


# Snowflake accepts these characters in an unquoted identifier.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_MAX_IDENTIFIER_LENGTH = 255

_ROUTED_NAME_PARTS = ("database_name", "schema_name", "table_name")

# Maximum width of a rule description in a CLI message.
_MAX_DESCRIPTION_LENGTH = 200

# A quoted dict key at the start of a source line, such as ``"dev":``.
_DICT_KEY_PREFIX = re.compile(r"""^(['"])[^'"]*\1\s*:\s*""")


class RoutingConfig(BaseModel):
    """Base class for all typed routing rules."""

    policy: str

    @abstractmethod
    def apply(self, logical_name: str) -> str:
        """Remap a logical full_name to its physical target.

        Args:
            logical_name: Filesystem-derived "database_name.schema_name.table_name".

        Returns:
            The routed full_name string. ``route()`` validates this value.
        """


class DatabaseOverrideRouting(RoutingConfig):
    """Replace the database component of every non-SOURCE Trouve's full_name."""

    policy: Literal["database_override"] = "database_override"
    database_name: str

    def apply(self, logical_name: str) -> str:
        _, schema_name, table_name = logical_name.split(".")
        return f"{self.database_name}.{schema_name}.{table_name}"


class SchemaIsolationRouting(RoutingConfig):
    """Collapse a name triple into one table token under a fixed database and schema."""

    policy: Literal["schema_isolation"] = "schema_isolation"
    database_name: str
    schema_name: str

    def apply(self, logical_name: str) -> str:
        database_name, schema_name, table_name = logical_name.split(".")
        collapsed_table_name = f"{database_name}_{schema_name}_{table_name}".upper()
        return f"{self.database_name}.{self.schema_name}.{collapsed_table_name}"


# A routing rule is either a typed config or a plain callable.
RoutingCallable = Callable[[str, str, str], str]
RoutingRule = RoutingConfig | RoutingCallable


def describe_routing(routing: RoutingRule | None) -> str:
    """Return a short human-readable description of a routing rule.

    The CLI prints this text in collision and validation messages. For a callable
    rule, the description is the source code of the rule, because the source is
    what tells you why two names collided.
    """
    if routing is None:
        return "none"
    if isinstance(routing, DatabaseOverrideRouting):
        return f"database_override → {routing.database_name}"
    if isinstance(routing, SchemaIsolationRouting):
        return f"schema_isolation → {routing.database_name}.{routing.schema_name}"
    if isinstance(routing, RoutingConfig):
        return routing.policy
    return _describe_callable(routing)


def _describe_callable(routing: RoutingCallable) -> str:
    """Return the source text of a callable rule, or its name as a fallback.

    A long rule is truncated, not dropped. Even a partial rule tells the reader
    more than the word "lambda".
    """
    try:
        source = inspect.getsource(routing).strip()
    except (OSError, TypeError):
        source = ""

    if source:
        one_line = " ".join(line.strip() for line in source.splitlines())
        one_line = _strip_dict_punctuation(one_line)
        if len(one_line) > _MAX_DESCRIPTION_LENGTH:
            return one_line[: _MAX_DESCRIPTION_LENGTH - 1] + "…"
        return one_line

    name = getattr(routing, "__name__", "")
    return name or repr(routing)


def _strip_dict_punctuation(source: str) -> str:
    """Remove the dict syntax around a rule that a routing table holds.

    ``inspect.getsource`` returns the whole line, so a lambda inside a dict
    arrives as ``"dev": lambda ...: (...),``. The key and the comma add noise.
    """
    stripped = source.strip().rstrip(",").strip()
    key_match = _DICT_KEY_PREFIX.match(stripped)
    if key_match:
        stripped = stripped[key_match.end():].strip()
    return stripped


def _apply_routing(logical_name: str, routing: RoutingRule) -> object:
    """Apply a routing rule and return its raw, not yet validated, result."""
    if isinstance(routing, RoutingConfig):
        return routing.apply(logical_name)

    database_name, schema_name, table_name = logical_name.split(".")
    try:
        return routing(database_name, schema_name, table_name)
    except InvalidRoutingConfigError:
        raise
    except Exception as exc:
        raise InvalidRoutingConfigError(
            f"The routing rule `{describe_routing(routing)}` failed on "
            f"'{logical_name}': {type(exc).__name__}: {exc}"
        ) from exc


def _validate_routed_name(
    routed_name: object, logical_name: str, routing: RoutingRule
) -> str:
    """Confirm that a routing rule returned a usable physical name.

    Args:
        routed_name: The raw value that the routing rule returned.
        logical_name: The name that clair gave to the rule.
        routing: The rule itself. Used for the error message.

    Returns:
        The routed name, as a validated string.

    Raises:
        InvalidRoutingConfigError: If the value is not a valid 3-part name.
    """
    rule_text = f"The routing rule `{describe_routing(routing)}`"

    if not isinstance(routed_name, str):
        raise InvalidRoutingConfigError(
            f"{rule_text} returned {type(routed_name).__name__} for '{logical_name}'. "
            "A routing rule must return a "
            "'database_name.schema_name.table_name' string."
        )

    parts = routed_name.split(".")
    if len(parts) != 3:
        raise InvalidRoutingConfigError(
            f"{rule_text} returned '{routed_name}' for '{logical_name}'. "
            f"A routed name needs 3 dot-separated parts, but this name has {len(parts)}."
        )

    for part_label, part in zip(_ROUTED_NAME_PARTS, parts):
        if len(part) > _MAX_IDENTIFIER_LENGTH:
            raise InvalidRoutingConfigError(
                f"{rule_text} returned the {part_label} '{part}' for '{logical_name}' "
                f"({len(part)} characters, maximum {_MAX_IDENTIFIER_LENGTH})."
            )
        if not _VALID_IDENTIFIER.match(part):
            raise InvalidRoutingConfigError(
                f"{rule_text} returned the invalid {part_label} '{part}' for "
                f"'{logical_name}'. An identifier starts with a letter or an "
                "underscore. The other characters are letters, digits, underscores "
                "or dollar signs."
            )

    return routed_name


def route(
    logical_name: str,
    trouve_type: TrouveType,
    routing: RoutingRule | None,
) -> str:
    """Apply a routing rule to a logical full_name.

    SOURCE Trouves always pass through, whatever the rule is.

    Args:
        logical_name: Filesystem-derived "database_name.schema_name.table_name".
        trouve_type: SOURCE, TABLE, or VIEW.
        routing: Active routing rule, or None for passthrough.

    Returns:
        The routed full_name string.

    Raises:
        InvalidRoutingConfigError: If the rule fails, or returns an unusable name.
    """
    if routing is None or trouve_type == TrouveType.SOURCE:
        return logical_name

    routed_name = _apply_routing(logical_name, routing)
    return _validate_routed_name(routed_name, logical_name, routing)


def collect_routing_problems(
    trouves: list[Trouve],
    routing: RoutingRule | None,
) -> list[tuple[str, str]]:
    """Apply a routing rule to every Trouve and collect all failures.

    ``route()`` stops at the first bad name, which is correct for a run. This
    function instead reports every problem at once, so that ``clair validate``
    shows a complete list.

    Args:
        trouves: All Trouves in the project, discovered with routing disabled.
        routing: The routing rule to test.

    Returns:
        List of ``(logical_name, problem_text)`` pairs, in discovery order.
    """
    if routing is None:
        return []

    problems: list[tuple[str, str]] = []
    for trouve in trouves:
        if not trouve.compiled:
            continue
        logical_name = trouve.compiled.logical_name
        try:
            route(logical_name, trouve.type, routing)
        except InvalidRoutingConfigError as exc:
            problems.append((logical_name, str(exc)))
    return problems


def detect_routing_collisions(logical_to_routed: dict[str, str]) -> list[tuple[str, list[str]]]:
    """Return (target, sources) pairs for any routing collisions.

    A collision occurs when two TABLE/VIEW Trouves route to the same physical target.
    The last write in execution order sets the final state of that target.

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
