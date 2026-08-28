"""The tests of the runner. They use RecordingAdapter, a complete in-memory adapter."""

from __future__ import annotations

from pathlib import Path

from structlog.testing import capture_logs

from clair.adapters.base import StatementStatus
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import RunStatus, RunSummary, run_project
from clair.trouves.run_config import RunMode
from tests.helpers import (
    DatabaseOverrideRouting,
    RecordingAdapter,
    make_run_result,
    make_statement,
)


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
        assert str(results[0].addresses.logical) == "analytics.revenue.daily_orders"
        assert str(results[0].addresses.physical) == "dev_db.revenue.daily_orders"

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
        assert results[0].addresses.logical == results[0].addresses.physical


class TestRunner:
    def test_run_simple_project(self, simple_project: Path):
        discovered = discover_project(simple_project)
        dag = build_dag(discovered)
        selected = get_executable_nodes(dag)

        adapter = RecordingAdapter()
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        assert len(results) == 1  # The TABLE only, not the SOURCE.
        assert str(results[0].addresses.physical) == "analytics.revenue.daily_orders"
        assert results[0].status == RunStatus.SUCCESS
        assert [s for s in results[0].statements if s.query_id]

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

        staging_result = next(r for r in results if r.addresses.matches("db.s.staging"))
        mart_result = next(r for r in results if r.addresses.matches("db.s.mart"))

        assert staging_result.status == RunStatus.FAILURE
        assert staging_result.error
        assert mart_result.status == RunStatus.SKIPPED
        assert mart_result.skipped_by == "db.s.staging"

    def test_the_summary_of_a_run_that_fails(self):
        results = [
            make_run_result(
                "db.s.staging",
                statements=[
                    make_statement(
                        "CREATE OR REPLACE TABLE db.s.staging AS (select 1)",
                        status=StatementStatus.FAILURE,
                        query_id="qid-001",
                        query_url="https://test/#/query/qid-001",
                        error="Object does not exist",
                    )
                ],
                error="Object does not exist",
                duration_seconds=0.5,
            ),
            make_run_result("db.s.mart", skipped_by="db.s.staging"),
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
            make_run_result("db.s.a", statements=[make_statement(query_id="q1")], duration_seconds=1.0),
            make_run_result("db.s.b", statements=[make_statement(query_id="q2")], duration_seconds=2.0),
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
            make_run_result("db.s.a", error="err1"),
            make_run_result("db.s.b", error="err2"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 2
        assert output.skipped_count == 0

    def test_all_skipped(self):
        results = [
            make_run_result("db.s.a", skipped_by="db.s.upstream"),
            make_run_result("db.s.b", skipped_by="db.s.upstream"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 2
        assert len(output.skipped) == 2

    def test_mixed_results(self):
        results = [
            make_run_result("db.s.ok", statements=[make_statement(query_id="q1")], duration_seconds=1.0),
            make_run_result("db.s.fail", error="broke"),
            make_run_result("db.s.skip", skipped_by="db.s.fail"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 1
        assert output.failed_count == 1
        assert output.skipped_count == 1
        assert str(output.succeeded[0].addresses.physical) == "db.s.ok"
        assert str(output.failed[0].addresses.physical) == "db.s.fail"
        assert str(output.skipped[0].addresses.physical) == "db.s.skip"

    def test_results_list_preserved_in_summary(self):
        results = [
            make_run_result("db.s.a", statements=[make_statement(query_id="q1")]),
        ]
        output = RunSummary(results=results, env_name="default")
        assert len(output.results) == 1
        assert str(output.results[0].addresses.physical) == "db.s.a"

    def test_is_run_summary_instance(self):
        from clair.core.runner import RunSummary

        output = RunSummary(results=[], env_name="default")
        assert isinstance(output, RunSummary)
