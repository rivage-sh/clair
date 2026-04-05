"""Global registry for Trouve placeholder resolution.

When a Trouve is used inside an f-string (e.g. ``f"SELECT * FROM {other_trouve}"``),
its ``__format__`` method registers it here and returns a placeholder token like
``__CLAIR_TROUVE_140234567890__``. Discovery later resolves these tokens to real full_names.
"""

from __future__ import annotations

TROUVE_PLACEHOLDER_PREFIX = "__CLAIR_TROUVE_"
THIS_PLACEHOLDER = "__CLAIR_THIS__"

# Maps id(Trouve) -> Trouve instance.
# Populated during module loading via Trouve.__format__.
_registry: dict[int, object] = {}


def register(trouve: object) -> str:
    """Register a Trouve and return its placeholder token."""
    _registry[id(trouve)] = trouve
    return f"{TROUVE_PLACEHOLDER_PREFIX}{id(trouve)}"


def clear() -> None:
    """Clear the registry. Called at the start of each discover_project() run."""
    _registry.clear()