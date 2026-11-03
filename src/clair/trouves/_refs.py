"""The global registry that clair uses to replace a Trouve placeholder.

Put a Trouve in an f-string, for example ``f"SELECT * FROM {other_trouve}"``.
Then its ``__format__`` method adds the Trouve to this registry and gives a
token, for example ``__CLAIR_TROUVE_140234567890``. Later, discovery replaces
each token with the true physical_address.

``other_trouve.unsampled()`` gives a token with the ``UNSAMPLED_`` marker, for
example ``__CLAIR_TROUVE_UNSAMPLED_140234567890``. A sampled run reads the
complete object for that token.
"""

from __future__ import annotations

TROUVE_PLACEHOLDER_PREFIX = "__CLAIR_TROUVE_"
UNSAMPLED_MARKER = "UNSAMPLED_"
THIS_PLACEHOLDER = "__CLAIR_THIS__"

# The registry maps id(Trouve) to the Trouve object.
# Trouve.__format__ fills it while Python loads the modules.
_registry: dict[int, object] = {}


def register(trouve: object, sampled: bool = True) -> str:
    """Add a Trouve to the registry and give its placeholder token.

    *sampled* False gives the token that a sampled run never filters.
    """
    _registry[id(trouve)] = trouve
    marker = "" if sampled else UNSAMPLED_MARKER
    return f"{TROUVE_PLACEHOLDER_PREFIX}{marker}{id(trouve)}"


def clear() -> None:
    """Empty the registry. discover_project() calls this before it starts."""
    _registry.clear()
