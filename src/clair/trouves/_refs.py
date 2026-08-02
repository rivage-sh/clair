"""The global registry that clair uses to replace a Trouve placeholder.

Put a Trouve in an f-string, for example ``f"SELECT * FROM {other_trouve}"``.
Then its ``__format__`` method adds the Trouve to this registry and gives a
token, for example ``__CLAIR_TROUVE_140234567890__``. Later, discovery replaces
each token with the true full_name.
"""

from __future__ import annotations

TROUVE_PLACEHOLDER_PREFIX = "__CLAIR_TROUVE_"
THIS_PLACEHOLDER = "__CLAIR_THIS__"

# The registry maps id(Trouve) to the Trouve object.
# Trouve.__format__ fills it while Python loads the modules.
_registry: dict[int, object] = {}


def register(trouve: object) -> str:
    """Add a Trouve to the registry and give its placeholder token."""
    _registry[id(trouve)] = trouve
    return f"{TROUVE_PLACEHOLDER_PREFIX}{id(trouve)}"


def clear() -> None:
    """Empty the registry. discover_project() calls this before it starts."""
    _registry.clear()