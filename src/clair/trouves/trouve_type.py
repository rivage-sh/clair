"""TrouveType -- the kind of warehouse object that a Trouve is.

The type has its own module, because routing reads it and needs nothing else of
a Trouve. ``RoutingEntry.route()`` takes the type, thus a project can keep each
SOURCE at the address that the file system gives.
"""

from __future__ import annotations

from enum import StrEnum


class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE = "table"
    VIEW = "view"
