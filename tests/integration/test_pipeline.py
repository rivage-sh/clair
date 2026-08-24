"""Run the pipeline project against Snowflake.

The tests run in the order of this file. Each test reads the objects that the
test before it made, thus the order matters.
"""

from __future__ import annotations

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.ci_snowflake import (
    SEED_DAILY_EVENT_COUNT_ROW_COUNT,
    SEED_EVENT_ROW_COUNT,
    SEED_ORDER_ROW_COUNT,
    SEED_RECENT_ORDER_ROW_COUNT,
    SEED_USER_COUNT,
    IntegrationConfig,
    row_count,
)
from tests.integration.conftest import (
    PIPELINE_PROJECT_PATH,
    log_events,
    logical_names_of,
    run_clair,
)

pytestmark = pytest.mark.integration


def physical_name(config: IntegrationConfig, schema_name: str, table_name: str) -> str:
    """Give the full physical name that the routing entry builds."""
    return f"{config.database_name}.{config.schema_prefix}_{schema_name}.{table_name}"


def test_full_refresh_run_writes_every_trouve(
    clair_environment: dict[str, str],
    snowflake_workspace: IntegrationConfig,
    snowflake_adapter: SnowflakeAdapter,
) -> None:
    """A full refresh makes each TABLE and each VIEW, and the tests pass."""
    completed = run_clair(
        ["run", "--project", str(PIPELINE_PROJECT_PATH), "--run-mode", "full_refresh"],
        clair_environment,
    )

    config = snowflake_workspace
    assert logical_names_of(completed, "run.node.success") == {
        "clair_ci.refined.events",
        "clair_ci.refined.orders",
        "clair_ci.derived.daily_event_counts",
        "clair_ci.derived.recent_orders",
        "clair_ci.derived.user_order_summary",
        "clair_ci.derived.user_purchase_summary",
    }

    assert (
        row_count(snowflake_adapter, physical_name(config, "REFINED", "EVENTS"))
        == SEED_EVENT_ROW_COUNT
    )
    assert (
        row_count(snowflake_adapter, physical_name(config, "REFINED", "ORDERS"))
        == SEED_ORDER_ROW_COUNT
    )
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "DAILY_EVENT_COUNTS"))
        == SEED_DAILY_EVENT_COUNT_ROW_COUNT
    )
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "RECENT_ORDERS"))
        == SEED_ORDER_ROW_COUNT
    )
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "USER_ORDER_SUMMARY"))
        == SEED_USER_COUNT
    )
    # The VIEW proves two things: clair routes a VIEW, and a Trouve reads two
    # schemas of one run.
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "USER_PURCHASE_SUMMARY"))
        == SEED_USER_COUNT
    )


def test_the_run_writes_the_routed_physical_names(
    clair_environment: dict[str, str], snowflake_workspace: IntegrationConfig
) -> None:
    """Each physical name in the log holds the schema prefix of this run."""
    completed = run_clair(
        ["run", "--project", str(PIPELINE_PROJECT_PATH), "--run-mode", "full_refresh"],
        clair_environment,
    )
    physical_names = {
        str(event["physical"])
        for event in log_events(completed)
        if event.get("event") == "run.node.success"
    }
    assert physical_names
    for name in physical_names:
        assert name.upper().startswith(
            f"{snowflake_workspace.database_name}.{snowflake_workspace.schema_prefix}_"
        )


def test_source_trouves_keep_their_logical_names(
    snowflake_workspace: IntegrationConfig, snowflake_adapter: SnowflakeAdapter
) -> None:
    """A SOURCE Trouve never routes, thus the run did not copy the seed tables."""
    config = snowflake_workspace
    assert (
        row_count(snowflake_adapter, f"{config.database_name}.SEED.EVENTS")
        == SEED_EVENT_ROW_COUNT
    )
    assert not snowflake_adapter.table_exists(
        config.database_name, f"{config.schema_prefix}_SEED", "EVENTS"
    )


def test_incremental_run_appends_and_merges(
    clair_environment: dict[str, str],
    snowflake_workspace: IntegrationConfig,
    snowflake_adapter: SnowflakeAdapter,
) -> None:
    """APPEND adds the recent rows. UPSERT merges, thus its row count stays."""
    run_clair(
        ["run", "--project", str(PIPELINE_PROJECT_PATH), "--run-mode", "incremental"],
        clair_environment,
    )

    config = snowflake_workspace
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "RECENT_ORDERS"))
        == SEED_ORDER_ROW_COUNT + SEED_RECENT_ORDER_ROW_COUNT
    )
    assert (
        row_count(snowflake_adapter, physical_name(config, "DERIVED", "USER_ORDER_SUMMARY"))
        == SEED_USER_COUNT
    )


def test_select_runs_one_schema_only(clair_environment: dict[str, str]) -> None:
    """--select limits the run to the Trouves that match the pattern."""
    completed = run_clair(
        [
            "run",
            "--project",
            str(PIPELINE_PROJECT_PATH),
            "--select",
            "clair_ci.refined.*",
            "--run-mode",
            "full_refresh",
        ],
        clair_environment,
    )
    assert logical_names_of(completed, "run.node.success") == {
        "clair_ci.refined.events",
        "clair_ci.refined.orders",
    }


def test_data_quality_tests_pass(clair_environment: dict[str, str]) -> None:
    """clair test runs each attached test against the objects of this run."""
    completed = run_clair(
        ["test", "--project", str(PIPELINE_PROJECT_PATH)], clair_environment
    )
    complete_events = [
        event for event in log_events(completed) if event.get("event") == "test.complete"
    ]
    assert complete_events, "clair test wrote no test.complete event"
    assert complete_events[-1]["failed"] == 0
    assert complete_events[-1]["errors"] == 0
    assert int(str(complete_events[-1]["passed"])) > 0
