"""Run each example project against Snowflake.

The projects in `examples/projects/` are the fixtures. A change that breaks an
example therefore breaks the build, and the documentation stays correct.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.config import DATABASE_NAME, IntegrationConfig
from tests.integration.conftest import clair_environment, events_named, run_clair
from tests.integration.projects import (
    copy_with_ci_routing,
    example_project_paths,
    model_logical_names,
    physical_table_name,
    trouves_of,
)
from tests.integration.warehouse import row_count, table_names_in_schema

pytestmark = pytest.mark.integration

EXAMPLE_PROJECT_PATHS = example_project_paths()
EXAMPLE_PROJECT_IDS = [path.name for path in EXAMPLE_PROJECT_PATHS]


@pytest.fixture(scope="module")
def project_copies(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """Copy each example project once, with the CI routing entry."""
    destination = tmp_path_factory.mktemp("projects")
    return {
        path.name: copy_with_ci_routing(path, destination)
        for path in EXAMPLE_PROJECT_PATHS
    }


@pytest.mark.parametrize("project_path", EXAMPLE_PROJECT_PATHS, ids=EXAMPLE_PROJECT_IDS)
def test_a_full_refresh_builds_every_model(
    project_path: Path,
    project_copies: dict[str, Path],
    snowflake_workspace: IntegrationConfig,
    adapter: SnowflakeAdapter,
    clair_home: Path,
) -> None:
    """Each Trouve that clair builds exists in the schema of the run."""
    copy_path = project_copies[project_path.name]
    environment = clair_environment(snowflake_workspace, clair_home)

    completed = run_clair(["run", "--project", str(copy_path)], environment)

    expected = {
        physical_table_name(logical_name)
        for logical_name in model_logical_names(trouves_of(project_path))
    }
    assert expected, f"{project_path.name} builds no Trouve"

    present = table_names_in_schema(
        adapter, DATABASE_NAME, snowflake_workspace.schema_name
    )
    assert expected <= present

    successes = events_named(completed, "run.node.success")
    assert len(successes) == len(expected)


@pytest.mark.parametrize("project_path", EXAMPLE_PROJECT_PATHS, ids=EXAMPLE_PROJECT_IDS)
def test_the_data_quality_tests_pass(
    project_path: Path,
    project_copies: dict[str, Path],
    snowflake_workspace: IntegrationConfig,
    clair_home: Path,
) -> None:
    """`clair test` reports no failure and no error."""
    copy_path = project_copies[project_path.name]
    environment = clair_environment(snowflake_workspace, clair_home)

    completed = run_clair(["test", "--project", str(copy_path)], environment)

    results = events_named(completed, "test.result")
    if not results:
        pytest.skip(f"{project_path.name} declares no data quality test")
    failed = [result for result in results if not result.get("passed")]
    assert failed == []


def test_the_incremental_append_adds_the_rows_of_the_window(
    project_copies: dict[str, Path],
    snowflake_workspace: IntegrationConfig,
    adapter: SnowflakeAdapter,
    clair_home: Path,
) -> None:
    """example_3 appends each order of the last 3 days.

    The full refresh takes all 6 seed orders. The incremental run then appends
    each order in the 3 day window: the 2 orders of yesterday, and the 2 orders
    that this test inserts. The window gives that overlap on purpose, thus a row
    that arrives late still reaches the table.
    """
    copy_path = project_copies["example_3"]
    environment = clair_environment(snowflake_workspace, clair_home)
    schema = f"{DATABASE_NAME}.{snowflake_workspace.schema_name}"
    recent_orders = f"{schema}.{physical_table_name('example_3_database.derived.recent_orders')}"
    source_orders = f"{schema}.{physical_table_name('example_3_database.source.orders')}"

    run_clair(["run", "--project", str(copy_path), "--run-mode", "full_refresh"], environment)
    assert row_count(adapter, recent_orders) == 6

    adapter.execute(f"""
        insert into {source_orders}
        select column1, column2, column3, column4, column5, column6
        from values
            ('ord_007', 'cust_c', 'placed',  55.00, current_timestamp(), current_timestamp()),
            ('ord_008', 'cust_d', 'placed', 310.00, current_timestamp(), current_timestamp())
    """)

    run_clair(
        ["run", "--project", str(copy_path), "--run-mode", "incremental"], environment
    )
    assert row_count(adapter, recent_orders) == 10


def test_select_builds_one_part_of_the_dag(
    project_copies: dict[str, Path],
    snowflake_workspace: IntegrationConfig,
    clair_home: Path,
) -> None:
    """`--select` builds the named Trouve only.

    A selector matches the physical name, thus the pattern holds the routed
    name, not the logical name.
    """
    copy_path = project_copies["example_1"]
    environment = clair_environment(snowflake_workspace, clair_home)
    selector = (
        f"{DATABASE_NAME}.{snowflake_workspace.schema_name}."
        f"{physical_table_name('example_1_database.refined.events')}"
    )

    completed = run_clair(
        ["run", "--project", str(copy_path), "--select", selector], environment
    )

    successes = events_named(completed, "run.node.success")
    assert len(successes) == 1
    assert successes[0].get("logical") == "example_1_database.refined.events"
