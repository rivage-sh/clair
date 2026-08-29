"""Find the example projects, and give each one a test routing entry.

The integration tests run the projects in `examples/projects/`. A test copies a
project to a temporary directory and writes a `__routing__.py` there, thus the
project in the repository keeps its own routing entry.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from clair import TrouveAddress
from clair.core.discovery import discover_project
from clair.trouves.trouve import TrouveAbc, TrouveType
from tests.integration.config import DATABASE_NAME
from tests.integration.routing_rule import make_table_name

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_PROJECTS_DIR = REPOSITORY_ROOT / "examples" / "projects"

# `clair init` writes this project, and tests/integration/test_init.py covers
# it. The example sweep skips it.
SKIPPED_PROJECT_NAMES = frozenset({"example_from_init"})

CI_ROUTING_FILE = '''\
"""The routing entry of the pull request tests.

Every Trouve goes to one schema of the test database, and a SOURCE Trouve is not
an exception. `tests.integration.routing_rule` holds the name rule, and the
assertions of a test read the same module. Thus one change moves both.

This file is not a project file that a user writes. clair reads it in the pytest
process, and `tests` is importable there because pytest puts the repository root
on `sys.path`. A copy of this project outside pytest cannot import the module,
and it does not need to.
"""

import os

from clair import RoutingEntry, RoutingTable, TrouveAddress, TrouveType
from tests.integration.routing_rule import make_table_name

DATABASE_NAME = "clair_pr_testing"


class PullRequestTestingRouting(RoutingEntry):
    """Send every Trouve to clair_pr_testing.<schema>.<prefix>__<db>__<schema>__<table>."""

    environment_name: str = "pr_testing"

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return TrouveAddress(
            database_name=DATABASE_NAME,
            schema_name=os.environ["CLAIR_PR_TESTING_SCHEMA_NAME"],
            table_name=make_table_name(
                trouve_address.database_name,
                trouve_address.schema_name,
                trouve_address.table_name,
            ),
        )


routing = RoutingTable(entries=[PullRequestTestingRouting()])
'''

# Each example names `compute_wh`, which the CI role cannot use. The copy reads
# the warehouse of the run instead, thus the per-database default still applies.
CI_DATABASE_CONFIG_FILE = '''\
import os

from clair import DatabaseDefaults

defaults = DatabaseDefaults(
    warehouse=os.environ["CLAIR_PR_TESTING_SNOWFLAKE_WAREHOUSE"],
)
'''


def example_project_paths() -> list[Path]:
    """Give the path of each example project, in name order."""
    return sorted(
        path
        for path in EXAMPLE_PROJECTS_DIR.iterdir()
        if path.is_dir() and path.name not in SKIPPED_PROJECT_NAMES
    )


def physical_table_name(logical_name: str) -> str:
    """Give the routed table name of one logical name.

    `routing_rule.make_table_name` holds the rule, and CI_ROUTING_FILE reads the
    same function. This function takes the three parts as one dotted text,
    because a test holds a logical name in that shape.
    """
    return make_table_name(*logical_name.split("."))


def physical_address(logical_name: str, schema_name: str) -> TrouveAddress:
    """Give the address that the test routing entry makes for one logical name."""
    return TrouveAddress(
        database_name=DATABASE_NAME,
        schema_name=schema_name,
        table_name=physical_table_name(logical_name),
    )


def golden_table_name(project_path: Path, logical_name: str) -> str:
    """Give the golden table that holds the rows of one SOURCE Trouve.

    `tests/integration/scripts/clair_pr_testing_setup.sql` makes one schema for each project, named
    after the project directory.
    """
    table_name = logical_name.split(".")[-1]
    return f"{project_path.name}.{table_name}"


def trouves_of(project_path: Path) -> list[TrouveAbc]:
    """Give each Trouve of one project, with the logical names."""
    return discover_project(project_path)


def source_logical_names(trouves: list[TrouveAbc]) -> list[str]:
    """Give the logical name of each SOURCE Trouve."""
    return [
        str(trouve.compiled.logical_address)
        for trouve in trouves
        if trouve.compiled is not None and trouve.type == TrouveType.SOURCE
    ]


def model_logical_names(trouves: list[TrouveAbc]) -> list[str]:
    """Give the logical name of each Trouve that clair builds."""
    return [
        str(trouve.compiled.logical_address)
        for trouve in trouves
        if trouve.compiled is not None and trouve.type != TrouveType.SOURCE
    ]


def copy_with_ci_routing(project_path: Path, destination: Path) -> Path:
    """Copy one project and give it the test routing entry.

    Returns:
        The path of the copy.
    """
    copy_path = destination / project_path.name
    shutil.copytree(
        project_path,
        copy_path,
        ignore=shutil.ignore_patterns("_clairtifacts", "__pycache__"),
    )
    (copy_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    for config_path in copy_path.rglob("__database_config__.py"):
        config_path.write_text(CI_DATABASE_CONFIG_FILE)
    return copy_path
