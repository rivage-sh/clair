"""Run each example project against Snowflake.

The projects in `examples/projects/` are the fixtures. A change that breaks an
example therefore breaks the build, and the documentation stays correct. A new
project in that directory joins these tests with no change here.

Each test calls `clair.run()` or `clair.test()` and reads the summary that the
call gives. The summary names each Trouve, its status, its statements and its
test results, thus a test asks the run what happened.

The test routing entry puts every Trouve, a SOURCE too, in one schema. For pull
request 32 the mapping is:

    example_1_database.refined.events
        -> clair_pr_testing.pr_32.<prefix>__example_1_database__refined__events
    example_3_database.source.orders
        -> clair_pr_testing.pr_32.<prefix>__example_3_database__source__orders

`physical_address(logical_name, schema_name)` builds that address. `<prefix>`
comes from the `workspace_prefix` fixture, and it isolates the tests of this
module from the tests of another module that builds the same project.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    copy_with_ci_routing,
    example_project_paths,
    model_logical_names,
    physical_address,
    trouves_of,
)
from tests.integration.warehouse import row_count, table_exists

pytestmark = pytest.mark.integration

EXAMPLE_PROJECT_PATHS = example_project_paths()
EXAMPLE_PROJECT_IDS = [path.name for path in EXAMPLE_PROJECT_PATHS]


@pytest.fixture(scope="class")
def project_copies(
    tmp_path_factory: pytest.TempPathFactory, example_sources: list[str]
) -> dict[str, Path]:
    """Copy each example project once, with the test routing entry.

    `example_sources` clones the SOURCE table of each project under the prefix
    of this module. A copy is useless without those tables.
    """
    destination = tmp_path_factory.mktemp("projects")
    return {
        path.name: copy_with_ci_routing(path, destination)
        for path in EXAMPLE_PROJECT_PATHS
    }


@pytest.mark.parametrize("project_path", EXAMPLE_PROJECT_PATHS, ids=EXAMPLE_PROJECT_IDS)
def test_a_full_refresh_builds_every_model(
    project_path: Path,
    project_copies: dict[str, Path],
    clair_environment: IntegrationConfig,
    environment: Environment,
    adapter: SnowflakeAdapter,
) -> None:
    """Each Trouve that clair builds exists in the schema of the run."""
    copy_path = project_copies[project_path.name]
    schema_name = clair_environment.schema_name

    summary = clair.run(copy_path, env=environment)

    logical_names = model_logical_names(trouves_of(project_path))
    assert logical_names, f"{project_path.name} builds no Trouve"

    absent = [
        str(physical_address(logical_name, schema_name))
        for logical_name in logical_names
        if not table_exists(adapter, physical_address(logical_name, schema_name))
    ]
    assert absent == []

    assert summary.failed == []
    assert summary.succeeded_count == len(logical_names)

    # The result of each Trouve holds the statements that Snowflake ran, and
    # the staging address that clair built at. A staged run writes there first,
    # thus the statements name that address.
    for result in summary.succeeded:
        assert result.addresses.staging is not None
        assert summary.run_id in str(result.addresses.staging)
        assert [s for s in result.statements if s.query_id]
        if result.statements:
            assert any(
                str(result.addresses.staging) in s.sql for s in result.statements
            )


def test_the_address_of_a_trouve_is_the_one_that_you_expect(
    snowflake_workspace: IntegrationConfig,
    workspace_prefix: str,
) -> None:
    """The test routing entry gives this exact address.

    The other tests build an address with `physical_address`. This test writes
    the answer out, thus a reader sees the shape with no indirection. The
    prefix of the workspace is the first part of each name.
    """
    schema_name = snowflake_workspace.schema_name

    assert str(physical_address("example_1_database.refined.events", schema_name)) == (
        f"clair_pr_testing.{schema_name}."
        f"{workspace_prefix}__example_1_database__refined__events"
    )
    assert str(physical_address("example_3_database.source.orders", schema_name)) == (
        f"clair_pr_testing.{schema_name}."
        f"{workspace_prefix}__example_3_database__source__orders"
    )
    assert str(
        physical_address("example_2_database.reports.top_customers", schema_name)
    ) == (
        f"clair_pr_testing.{schema_name}."
        f"{workspace_prefix}__example_2_database__reports__top_customers"
    )


@pytest.mark.parametrize("project_path", EXAMPLE_PROJECT_PATHS, ids=EXAMPLE_PROJECT_IDS)
def test_the_data_quality_tests_pass(
    project_path: Path,
    project_copies: dict[str, Path],
    clair_environment: IntegrationConfig,
    environment: Environment,
) -> None:
    """`clair.test()` reports no failure and no error."""
    copy_path = project_copies[project_path.name]

    summary = clair.test(copy_path, env=environment)

    # Each example project must declare a minimum of one data quality test. An
    # example with no test gives this test nothing to do, and the run stays
    # green with no proof.
    assert summary.results, f"{project_path.name} declares no data quality test"
    assert summary.failed_results == []
    assert summary.error_count == 0


def test_the_incremental_append_adds_only_the_new_rows(
    project_copies: dict[str, Path],
    clair_environment: IntegrationConfig,
    environment: Environment,
    adapter: SnowflakeAdapter,
) -> None:
    """example_3 appends the orders of the last 3 days.

    Each date in the golden table is old, thus no seed order reaches the 3 day
    window. The full refresh takes all 6 orders, and the incremental run then
    appends the 2 orders that this test inserts.
    """
    copy_path = project_copies["example_3"]
    schema_name = clair_environment.schema_name
    recent_orders = physical_address(
        "example_3_database.derived.recent_orders", schema_name
    )
    source_orders = physical_address("example_3_database.source.orders", schema_name)

    clair.run(copy_path, env=environment, run_mode=RunMode.FULL_REFRESH)
    assert row_count(adapter, recent_orders) == 6

    adapter.execute(f"""
        insert into {source_orders}
        select column1, column2, column3, column4, column5, column6
        from values
            ('ord_007', 'cust_c', 'placed',  55.00, current_timestamp(), current_timestamp()),
            ('ord_008', 'cust_d', 'placed', 310.00, current_timestamp(), current_timestamp())
    """)

    summary = clair.run(copy_path, env=environment, run_mode=RunMode.INCREMENTAL)
    assert row_count(adapter, recent_orders) == 8

    # The mode of the result is the mode that clair used, after the RunConfig of
    # the Trouve and the fallback of a table that does not exist yet.
    result = summary.result("example_3_database.derived.recent_orders")
    assert result is not None
    assert result.effective_run_mode == RunMode.INCREMENTAL


def test_select_builds_one_part_of_the_dag(
    project_copies: dict[str, Path],
    clair_environment: IntegrationConfig,
    environment: Environment,
) -> None:
    """`select` builds the named Trouve only.

    A selector matches the physical address, thus the pattern is
    `clair_pr_testing.<schema>.example_1_database__refined__events`, not the
    logical name `example_1_database.refined.events`.
    """
    copy_path = project_copies["example_1"]
    selector = str(
        physical_address(
            "example_1_database.refined.events", clair_environment.schema_name
        )
    )

    summary = clair.run(copy_path, env=environment, select=[selector])

    assert summary.succeeded_count == 1
    assert str(summary.results[0].addresses.logical) == "example_1_database.refined.events"
