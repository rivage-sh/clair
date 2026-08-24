"""Run a project that `clair init` made, against Snowflake.

The test does what a new user does: it makes a project in a spare directory, it
names a source table that exists, it adds one Trouve, and it runs the project.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.ci_snowflake import SEED_ORDER_ROW_COUNT, IntegrationConfig, row_count
from tests.integration.conftest import (
    ENVIRONMENT_NAME,
    logical_names_of,
    make_clair_environment,
    run_clair,
)

pytestmark = pytest.mark.integration

# The routing file of the scaffold routes nothing. The test replaces it with the
# rule of the CI runs: put every write in the schemas of this run.
CI_ROUTING_FILE = '''"""Clair routing -- one entry that gives each CI run its own schemas."""

import os

from clair import RoutingEntry, RoutingTable, TrouveAddress


class ContinuousIntegrationRouting(RoutingEntry):
    """Put every write of one CI run in schemas that share one prefix."""

    environment_name: str = "ci"

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        schema_prefix = os.environ["CLAIR_CI_SCHEMA_PREFIX"]
        return trouve_address.model_copy(
            update={"schema_name": f"{schema_prefix}_{trouve_address.schema_name}"}
        )


routing = RoutingTable(entries=[ContinuousIntegrationRouting()])
'''

# One Trouve downstream of the scaffolded SOURCE. The scaffold writes the SOURCE
# only, thus a run needs this file to have work to do. The schema is "scaffold",
# so this table does not collide with the tables of the pipeline project.
SCAFFOLD_ORDERS_FILE = '''from clair_ci.seed.orders import trouve as clair_ci_seed_orders

from clair import Column, ColumnType, TestUnique, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The orders of the scaffolded project, with the order date.",
    sql=f"""
        select
            order_id,
            user_id,
            amount,
            created_at::date as created_date
        from {clair_ci_seed_orders}
    """,
    columns=[
        Column(name="order_id", type=ColumnType.STRING),
        Column(name="user_id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.FLOAT),
        Column(name="created_date", type=ColumnType.DATE),
    ],
    tests=[TestUnique(column="order_id")],
)
'''


def init_answers(config: IntegrationConfig) -> str:
    """Give the answers of the `clair init` prompts, for the CI credentials."""
    answers = [ENVIRONMENT_NAME, config.account, config.user]
    if config.private_key_path and config.private_key_passphrase:
        answers += ["1", config.private_key_path, "y", config.private_key_passphrase]
    elif config.private_key_path:
        answers += ["1", config.private_key_path, "n"]
    else:
        answers += ["2", config.password or ""]
    answers += [
        config.warehouse,
        config.role,
        "us-east-1",  # The region and the locator make the query URLs only.
        "abc12345",
        "clair_ci.seed.orders",
    ]
    return "\n".join(answers) + "\n"


def test_a_scaffolded_project_runs_against_snowflake(
    tmp_path: Path,
    snowflake_workspace: IntegrationConfig,
    snowflake_adapter: SnowflakeAdapter,
) -> None:
    """`clair init`, then one Trouve, then `clair run` writes a real table."""
    config = snowflake_workspace
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    environment = make_clair_environment(home_dir, config)
    project_dir = tmp_path / "spare_project"

    run_clair(
        ["init", "--project", str(project_dir)],
        environment,
        stdin_text=init_answers(config),
    )

    # The scaffold made clair_ci/seed/orders.py for the source table above.
    assert (project_dir / "clair_ci" / "seed" / "orders.py").is_file()

    (project_dir / "__routing__.py").write_text(CI_ROUTING_FILE)
    scaffold_dir = project_dir / "clair_ci" / "scaffold"
    scaffold_dir.mkdir(parents=True, exist_ok=True)
    (scaffold_dir / "orders.py").write_text(SCAFFOLD_ORDERS_FILE)

    completed = run_clair(["run", "--project", str(project_dir)], environment)
    assert logical_names_of(completed, "run.node.success") == {"clair_ci.scaffold.orders"}

    assert (
        row_count(
            snowflake_adapter,
            f"{config.database_name}.{config.schema_prefix}_SCAFFOLD.ORDERS",
        )
        == SEED_ORDER_ROW_COUNT
    )
