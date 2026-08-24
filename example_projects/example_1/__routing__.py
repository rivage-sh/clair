"""Clair routing -- gives each environment its physical write target.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress. SOURCE Trouves never route.

Run `clair validate --project example_projects/example_1` to find a rule that
gives an invalid name, and two Trouves that go to one target.
"""

import os
from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress


class EnvironmentName(StrEnum):
    """The environments of this project.

    Each member matches a top-level key in ~/.clair/environments.yml, and each
    one matches the environment_name of an entry below.
    """

    DEV = "dev"
    PROD = "prod"


class DeveloperRouting(RoutingEntry):
    """Each person writes to a separate database.

    With CLAIR_USER=alice, example_1_database.refined.events becomes
    alice.refined.events.
    """

    environment_name: str = EnvironmentName.DEV.value
    user_variable: str = "CLAIR_USER"

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        user_name = os.environ[self.user_variable]
        return trouve_address.model_copy(update={"database_name": user_name})


class ProductionRouting(RoutingEntry):
    """Production writes to the logical names, so the address stays the same."""

    environment_name: str = EnvironmentName.PROD.value

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DeveloperRouting(), ProductionRouting()])
