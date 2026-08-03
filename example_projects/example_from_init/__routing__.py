"""Clair routing -- gives each environment its physical write target.

Each entry names one environment. The name matches a top-level key in
~/.clair/environments.yml. The route method accepts the logical TrouveAddress
and gives the physical TrouveAddress. SOURCE Trouves never route.

Commit this file. It holds no credentials.
Run `clair validate` to apply the entries to every Trouve in the project.
"""

import os

from clair import RoutingEntry, RoutingTable, TrouveAddress


class DeveloperRouting(RoutingEntry):
    """Each person writes to a separate database.

    Set CLAIR_USER to your name before you run clair.
    """

    environment_name: str = "dev"
    user_variable: str = "CLAIR_USER"

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        user_name = os.environ[self.user_variable].upper()
        return trouve_address.model_copy(
            update={"database_name": f"{trouve_address.database_name}_{user_name}"}
        )


class ProductionRouting(RoutingEntry):
    """Production writes to the logical names, so the address stays the same."""

    environment_name: str = "prod"

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DeveloperRouting(), ProductionRouting()])
