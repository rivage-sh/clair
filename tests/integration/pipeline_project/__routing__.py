"""Clair routing for the integration test project.

Each CI run gets its own schema prefix. The prefix keeps the objects of one pull
request separate from the objects of every other pull request, because all runs
share one Snowflake database.

The prefix comes from CLAIR_CI_SCHEMA_PREFIX, for example ``PR_42``. The logical
schema ``refined`` then becomes the physical schema ``PR_42_REFINED``.

SOURCE Trouves never route, thus the seed tables keep the logical names
``clair_ci.seed.*``. The seed data is deterministic, so two runs at the same
time read the same rows.
"""

import os
from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress

SCHEMA_PREFIX_VARIABLE = "CLAIR_CI_SCHEMA_PREFIX"


class EnvironmentName(StrEnum):
    """The environments of this project."""

    CI = "ci"


class ContinuousIntegrationRouting(RoutingEntry):
    """Put every write of one CI run in schemas that share one prefix."""

    environment_name: str = EnvironmentName.CI.value
    schema_prefix_variable: str = SCHEMA_PREFIX_VARIABLE

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        schema_prefix = os.environ[self.schema_prefix_variable]
        return trouve_address.model_copy(
            update={"schema_name": f"{schema_prefix}_{trouve_address.schema_name}"}
        )


routing = RoutingTable(entries=[ContinuousIntegrationRouting()])
