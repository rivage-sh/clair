"""The tests of the runner. They use a false adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

from structlog.testing import capture_logs

from clair.adapters.base import QueryResult, WarehouseAdapter
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import RunResult, RunStatus, format_run_output, run_project
from clair.trouves.run_config import RunMode
from tests.helpers import DatabaseOverrideRouting


def _make_mock_adapter(fail_on: set[str] | None = None) -> WarehouseAdapter:
    """Make a false adapter. By default it succeeds, but it can fail on some Trouves."""
    fail_on = fail_on or set()
    adapter = MagicMock(spec=WarehouseAdapter)

    call_count = 0

    def mock_execute(sql: str) -> QueryResult:
        nonlocal call_count
        call_count += 1
        query_id = f"test-qid-{call_count:04d}"

        # Look for a fail_on name in the SQL.
        for name in fail_on:
            if name in sql:
                return QueryResult(
                    query_id=query_id,
                    query_url=f"https://test.snowflake.com/#/query/{query_id}",
                    success=False,
                    error=f"Simulated failure for {name}",
                )

        return QueryResult(
            query_id=query_id,
            query_url=f"https://test.snowflake.com/#/query/{query_id}",
            success=True,
        )

    adapter.execute.side_effect = mock_execute
    adapter.table_exists.return_value = True
    return adapter


class TestRunLogNames:
    """The logs and the results show the logical name and the physical name."""

    def test_log_shows_both_names(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="dev_db")
        discovered = discover_project(simple_project, routing=routing)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = _make_mock_adapter()
        with capture_logs() as log_entries:
            results = list(
                run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test")
            )

        for event_name in ("run.node.start", "run.node.success"):
            events = [e for e in log_entries if e["event"] == event_name]
            assert len(events) == 1, event_name
            assert events[0]["logical"] == "analytics.revenue.daily_orders"
            assert events[0]["physical"] == "dev_db.revenue.daily_orders"

        # The result carries both names too, thus the caller does not look
        # them up again.
        assert len(results) == 1
        assert results[0].logical_name == "analytics.revenue.daily_orders"
        assert results[0].physical_name == "dev_db.revenue.daily_orders"

    def test_the_two_names_are_equal_without_routing(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = _make_mock_adapter()
        with capture_logs() as log_entries:
            results = list(
                run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test")
            )

        start_events = [e for e in log_entries if e["event"] == "run.node.start"]
        assert len(start_events) == 1
        assert start_events[0]["logical"] == "analytics.revenue.daily_orders"
        assert start_events[0]["physical"] == "analytics.revenue.daily_orders"
        assert results[0].logical_name == results[0].physical_name


class TestRunner:
    def test_run_simple_project(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = _make_mock_adapter()
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        assert len(results) == 1  # The TABLE only, not the SOURCE.
        assert results[0].physical_name == "analytics.revenue.daily_orders"
        assert results[0].status == RunStatus.SUCCESS
        assert len(results[0].query_ids) > 0

    def test_run_executes_create_or_replace(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = _make_mock_adapter()
        list(run_project(dag, selected, adapter))

        # Examine the SQL that the code gave to execute.
        call_args = cast(Any, adapter.execute).call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in call_args
        assert "analytics.revenue.daily_orders" in call_args

    def test_format_run_output_success(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = _make_mock_adapter()
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))
        output = format_run_output(results, "default")

        assert output.succeeded_count == 1
        assert output.failed_count == 0
        assert output.skipped_count == 0
        assert output.env_name == "default"


class TestRunnerFailureHandling:
    def test_downstream_skipped_on_failure(self):
        """When a Trouve fails, clair skips each Trouve downstream of it."""
        from clair.trouves.config import ResolvedConfig
        from clair.trouves.trouve import (
            CompiledAttributes,
            ExecutionType,
            Trouve,
            TrouveType,
        )

        trouves = []
        for name, ttype, imports, sql in [
            ("db.s.source", TrouveType.SOURCE, [], ""),
            ("db.s.staging", TrouveType.TABLE, ["db.s.source"], "select * from db.s.source"),
            ("db.s.mart", TrouveType.TABLE, ["db.s.staging"], "select * from db.s.staging"),
        ]:
            t = Trouve(type=ttype, sql=sql) if sql else Trouve(type=ttype)
            t.compiled = CompiledAttributes(
                physical_name=name,
                logical_name=name,
                resolved_sql=sql,
                file_path=Path(f"/fake/{name}.py"),
                module_name=name,
                imports=imports,
                config=ResolvedConfig(),
                execution_type=ExecutionType.SNOWFLAKE,
            )
            trouves.append(t)

        dag = build_dag(trouves)
        selected = get_executable_nodes(dag)

        # Make staging fail.
        adapter = _make_mock_adapter(fail_on={"db.s.staging"})
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        staging_result = next(r for r in results if r.physical_name == "db.s.staging")
        mart_result = next(r for r in results if r.physical_name == "db.s.mart")

        assert staging_result.status == RunStatus.FAILURE
        assert staging_result.error
        assert mart_result.status == RunStatus.SKIPPED
        assert mart_result.skipped_by == "db.s.staging"

    def test_format_run_output_with_failure(self):
        results = [
            RunResult(
                physical_name="db.s.staging",
                status=RunStatus.FAILURE,
                query_ids=["qid-001"],
                query_urls=["https://test/#/query/qid-001"],
                error="Object does not exist",
                sql=["CREATE OR REPLACE TABLE db.s.staging AS (select 1)"],
                duration_seconds=0.5,
            ),
            RunResult(
                physical_name="db.s.mart",
                status=RunStatus.SKIPPED,
                skipped_by="db.s.staging",
            ),
        ]

        output = format_run_output(results, "default")
        assert output.succeeded_count == 0
        assert output.failed_count == 1
        assert output.skipped_count == 1


class TestRunSummaryProperties:
    """The tests of the RunSummary properties."""

    def test_empty_results_all_counts_zero(self):
        output = format_run_output([], "test_env")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 0

    def test_empty_results_list_properties_empty(self):
        output = format_run_output([], "test_env")
        assert output.succeeded == []
        assert output.failed == []
        assert output.skipped == []

    def test_env_name_preserved(self):
        output = format_run_output([], "my_env")
        assert output.env_name == "my_env"

    def test_all_succeeded(self):
        results = [
            RunResult(physical_name="db.s.a", status=RunStatus.SUCCESS, query_ids=["q1"], duration_seconds=1.0),
            RunResult(physical_name="db.s.b", status=RunStatus.SUCCESS, query_ids=["q2"], duration_seconds=2.0),
        ]
        output = format_run_output(results, "default")
        assert output.succeeded_count == 2
        assert output.failed_count == 0
        assert output.skipped_count == 0
        assert len(output.succeeded) == 2
        assert output.failed == []
        assert output.skipped == []

    def test_all_failed(self):
        results = [
            RunResult(physical_name="db.s.a", status=RunStatus.FAILURE, error="err1"),
            RunResult(physical_name="db.s.b", status=RunStatus.FAILURE, error="err2"),
        ]
        output = format_run_output(results, "default")
        assert output.succeeded_count == 0
        assert output.failed_count == 2
        assert output.skipped_count == 0

    def test_all_skipped(self):
        results = [
            RunResult(physical_name="db.s.a", status=RunStatus.SKIPPED, skipped_by="db.s.upstream"),
            RunResult(physical_name="db.s.b", status=RunStatus.SKIPPED, skipped_by="db.s.upstream"),
        ]
        output = format_run_output(results, "default")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 2
        assert len(output.skipped) == 2

    def test_mixed_results(self):
        results = [
            RunResult(physical_name="db.s.ok", status=RunStatus.SUCCESS, query_ids=["q1"], duration_seconds=1.0),
            RunResult(physical_name="db.s.fail", status=RunStatus.FAILURE, error="broke"),
            RunResult(physical_name="db.s.skip", status=RunStatus.SKIPPED, skipped_by="db.s.fail"),
        ]
        output = format_run_output(results, "default")
        assert output.succeeded_count == 1
        assert output.failed_count == 1
        assert output.skipped_count == 1
        assert output.succeeded[0].physical_name == "db.s.ok"
        assert output.failed[0].physical_name == "db.s.fail"
        assert output.skipped[0].physical_name == "db.s.skip"

    def test_results_list_preserved_in_summary(self):
        results = [
            RunResult(physical_name="db.s.a", status=RunStatus.SUCCESS, query_ids=["q1"]),
        ]
        output = format_run_output(results, "default")
        assert len(output.results) == 1
        assert output.results[0].physical_name == "db.s.a"

    def test_is_run_summary_instance(self):
        from clair.core.runner import RunSummary

        output = format_run_output([], "default")
        assert isinstance(output, RunSummary)
