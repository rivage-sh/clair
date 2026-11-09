"""Clair routing -- gives each environment its physical write target.

This file also marks the root of the clair project. Each clair command walks up
from the working directory to this file, in the same way that git finds .git.
Keep the file at the project root, also when it holds no entry.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress.

This file starts with one entry, and that entry changes nothing: the physical
address stays equal to the logical address, which is the production address.
Change the route method for a separate target for an environment, for example
one database for each person.
See https://rivage-sh.github.io/clair/topics/routing/
"""

from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType


class EnvironmentName(StrEnum):
    """The environments of this project.

    Each member matches a top-level key in ~/.clair/environments.yml.
    """

    DEV = "dev"


class PassthroughRouting(RoutingEntry):
    """Write to the logical address, thus the address stays the same."""

    environment_name: str = EnvironmentName.DEV.value

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[PassthroughRouting()])
