"""Clair routing -- gives each environment its physical write target.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress. The entry sees every Trouve, and
a SOURCE Trouve is not an exception.

This file starts with one entry, and that entry changes nothing: the physical
name stays equal to the logical name. Change the route method when you want a
separate target for an environment, for example one database for each person.
See https://clair.rivage.sh/guides/routing/
"""

from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class EnvironmentName(StrEnum):
    """The environments of this project.

    Each member matches a top-level key in ~/.clair/environments.yml.
    """

    DEV = "dev"


class DevelopmentRouting(RoutingEntry):
    """Write to the logical names, thus the address stays the same."""

    environment_name: str = EnvironmentName.DEV.value

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DevelopmentRouting()])
