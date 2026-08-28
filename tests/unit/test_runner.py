"""The tests of the runner. They use RecordingAdapter, a complete in-memory adapter."""

from __future__ import annotations

from pathlib import Path

from structlog.testing import capture_logs

from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import RunResult, RunStatus, RunSummary, run_project
from clair.trouves.run_config import RunMode
from tests.helpers import DatabaseOverrideRouting, RecordingAdapter


class TestRunLogNames:
    """The logs and the results show the logical address and the physical address."""

    def test_log_shows_both_names(self, simple_project: Path):
        routing = DatabaseOverrideRouting(database_name="dev_db")
        discovered = discover_project(simple_project, routing=routing)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
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
        assert results[0].logical_address == "analytics.revenue.daily_orders"
        assert results[0].physical_address == "dev_db.revenue.daily_orders"

    def test_the_two_names_are_equal_without_routing(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
        with capture_logs() as log_entries:
            results = list(
                run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test")
            )

        start_events = [e for e in log_entries if e["event"] == "run.node.start"]
        assert len(start_events) == 1
        assert start_events[0]["logical"] == "analytics.revenue.daily_orders"
        assert start_events[0]["physical"] == "analytics.revenue.daily_orders"
        assert results[0].logical_address == results[0].physical_address


class TestRunner:
    def test_run_simple_project(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        assert len(results) == 1  # The TABLE only, not the SOURCE.
        assert results[0].physical_address == "analytics.revenue.daily_orders"
        assert results[0].status == RunStatus.SUCCESS
        assert len(results[0].query_ids) > 0

    def test_run_executes_create_or_replace(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
        list(run_project(dag, selected, adapter))

        # Examine the SQL that the code gave to execute.
        call_args = adapter.record.statements[-1]
        assert "CREATE OR REPLACE TABLE" in call_args
        assert "analytics.revenue.daily_orders" in call_args

    def test_the_summary_of_a_run_that_succeeds(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))
        output = RunSummary(results=results, env_name="default")

        assert output.succeeded_count == 1
        assert output.failed_count == 0
        assert output.skipped_count == 0
        assert output.env_name == "default"


class TestRunnerFailureHandling:
    def test_downstream_skipped_on_failure(self):
        """When a Trouve fails, clair skips each Trouve downstream of it."""
        from clair.trouves.address import TrouveAddress
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
                physical_address=TrouveAddress.parse(name),
                logical_address=TrouveAddress.parse(name),
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
        adapter = RecordingAdapter(fail_on=["db.s.staging"])
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        staging_result = next(r for r in results if r.physical_address == "db.s.staging")
        mart_result = next(r for r in results if r.physical_address == "db.s.mart")

        assert staging_result.status == RunStatus.FAILURE
        assert staging_result.error
        assert mart_result.status == RunStatus.SKIPPED
        assert mart_result.skipped_by == "db.s.staging"

    def test_the_summary_of_a_run_that_fails(self):
        results = [
            RunResult(
                physical_address="db.s.staging",
                status=RunStatus.FAILURE,
                query_ids=["qid-001"],
                query_urls=["https://test/#/query/qid-001"],
                error="Object does not exist",
                sql=["CREATE OR REPLACE TABLE db.s.staging AS (select 1)"],
                duration_seconds=0.5,
            ),
            RunResult(
                physical_address="db.s.mart",
                status=RunStatus.SKIPPED,
                skipped_by="db.s.staging",
            ),
        ]

        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 1
        assert output.skipped_count == 1


class TestRunSummaryProperties:
    """The tests of the RunSummary properties."""

    def test_empty_results_all_counts_zero(self):
        output = RunSummary(results=[], env_name="test_env")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 0

    def test_empty_results_list_properties_empty(self):
        output = RunSummary(results=[], env_name="test_env")
        assert output.succeeded == []
        assert output.failed == []
        assert output.skipped == []

    def test_env_name_preserved(self):
        output = RunSummary(results=[], env_name="my_env")
        assert output.env_name == "my_env"

    def test_all_succeeded(self):
        results = [
            RunResult(physical_address="db.s.a", status=RunStatus.SUCCESS, query_ids=["q1"], duration_seconds=1.0),
            RunResult(physical_address="db.s.b", status=RunStatus.SUCCESS, query_ids=["q2"], duration_seconds=2.0),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 2
        assert output.failed_count == 0
        assert output.skipped_count == 0
        assert len(output.succeeded) == 2
        assert output.failed == []
        assert output.skipped == []

    def test_all_failed(self):
        results = [
            RunResult(physical_address="db.s.a", status=RunStatus.FAILURE, error="err1"),
            RunResult(physical_address="db.s.b", status=RunStatus.FAILURE, error="err2"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 2
        assert output.skipped_count == 0

    def test_all_skipped(self):
        results = [
            RunResult(physical_address="db.s.a", status=RunStatus.SKIPPED, skipped_by="db.s.upstream"),
            RunResult(physical_address="db.s.b", status=RunStatus.SKIPPED, skipped_by="db.s.upstream"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 2
        assert len(output.skipped) == 2

    def test_mixed_results(self):
        results = [
            RunResult(physical_address="db.s.ok", status=RunStatus.SUCCESS, query_ids=["q1"], duration_seconds=1.0),
            RunResult(physical_address="db.s.fail", status=RunStatus.FAILURE, error="broke"),
            RunResult(physical_address="db.s.skip", status=RunStatus.SKIPPED, skipped_by="db.s.fail"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 1
        assert output.failed_count == 1
        assert output.skipped_count == 1
        assert output.succeeded[0].physical_address == "db.s.ok"
        assert output.failed[0].physical_address == "db.s.fail"
        assert output.skipped[0].physical_address == "db.s.skip"

    def test_results_list_preserved_in_summary(self):
        results = [
            RunResult(physical_address="db.s.a", status=RunStatus.SUCCESS, query_ids=["q1"]),
        ]
        output = RunSummary(results=results, env_name="default")
        assert len(output.results) == 1
        assert output.results[0].physical_address == "db.s.a"

    def test_is_run_summary_instance(self):
        from clair.core.runner import RunSummary

        output = RunSummary(results=[], env_name="default")
        assert isinstance(output, RunSummary)
