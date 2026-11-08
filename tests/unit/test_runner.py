"""The tests of the runner. They use RecordingAdapter, a complete in-memory adapter."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

import clair
from clair.adapters.base import StatementStatus
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import RunFailure, RunSkip, RunStatus, RunSuccess, RunSummary, run_project
from clair.trouves.run_config import RunMode
from tests.helpers import (
    DatabaseOverrideRouting,
    RecordingAdapter,
    compile_trouve,
    make_environment,
    make_run_failure,
    make_run_skip,
    make_run_success,
    make_statement,
    make_trouve,
    write_project_marker,
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
        builds = [sql for sql in adapter.record.statements if "CREATE OR REPLACE TABLE" in sql]
        assert len(builds) == 1
        assert "analytics.revenue.daily_orders" in builds[0]

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
        from clair.trouves.trouve import TrouveType

        trouves = []
        for name, ttype, imports, sql in [
            ("db.s.source", TrouveType.SOURCE, [], ""),
            ("db.s.staging", TrouveType.TABLE, ["db.s.source"], "select * from db.s.source"),
            ("db.s.mart", TrouveType.TABLE, ["db.s.staging"], "select * from db.s.staging"),
        ]:
            trouves.append(compile_trouve(make_trouve(ttype, sql), name, imports=imports))

        dag = build_dag(trouves)
        selected = get_executable_nodes(dag)

        # Make staging fail.
        adapter = RecordingAdapter(fail_on=["db.s.staging"])
        results = list(run_project(dag, selected, adapter, run_mode=RunMode.FULL_REFRESH, run_id="test"))

        staging_result = next(r for r in results if r.addresses.matches("db.s.staging"))
        mart_result = next(r for r in results if r.addresses.matches("db.s.mart"))

        assert isinstance(staging_result, RunFailure)
        assert staging_result.error
        assert isinstance(mart_result, RunSkip)
        assert mart_result.skipped_by == "db.s.staging"

    def test_the_summary_of_a_run_that_fails(self):
        results = [
            make_run_failure(
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
            make_run_skip("db.s.mart", skipped_by="db.s.staging"),
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
            make_run_success("db.s.a", statements=[make_statement(query_id="q1")], duration_seconds=1.0),
            make_run_success("db.s.b", statements=[make_statement(query_id="q2")], duration_seconds=2.0),
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
            make_run_failure("db.s.a", error="err1"),
            make_run_failure("db.s.b", error="err2"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 2
        assert output.skipped_count == 0

    def test_all_skipped(self):
        results = [
            make_run_skip("db.s.a", skipped_by="db.s.upstream"),
            make_run_skip("db.s.b", skipped_by="db.s.upstream"),
        ]
        output = RunSummary(results=results, env_name="default")
        assert output.succeeded_count == 0
        assert output.failed_count == 0
        assert output.skipped_count == 2
        assert len(output.skipped) == 2

    def test_mixed_results(self):
        results = [
            make_run_success("db.s.ok", statements=[make_statement(query_id="q1")], duration_seconds=1.0),
            make_run_failure("db.s.fail", error="broke"),
            make_run_skip("db.s.skip", skipped_by="db.s.fail"),
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
            make_run_success("db.s.a", statements=[make_statement(query_id="q1")]),
        ]
        output = RunSummary(results=results, env_name="default")
        assert len(output.results) == 1
        assert str(output.results[0].addresses.physical) == "db.s.a"

    def test_is_run_summary_instance(self):
        from clair.core.runner import RunSummary

        output = RunSummary(results=[], env_name="default")
        assert isinstance(output, RunSummary)


class TestTheOutcomeOfAResult:
    """Each outcome is its own class, and it holds only the fields of that outcome."""

    def test_a_failure_without_a_cause_raises(self):
        with pytest.raises(ValidationError, match="error"):
            make_run_failure("db.s.a", error="")

    def test_a_skip_holds_no_field_of_a_run(self):
        skip = make_run_skip("db.s.b", skipped_by="db.s.a")

        assert set(skip.model_dump()) == {"addresses", "status", "skipped_by"}

    def test_the_summary_reads_each_outcome_back_from_json(self):
        summary = RunSummary(
            results=[
                make_run_success("db.s.a", row_count=3),
                make_run_failure("db.s.b", error="broke"),
                make_run_skip("db.s.c", skipped_by="db.s.b"),
            ],
            env_name="default",
        )

        read_back = RunSummary.model_validate_json(summary.model_dump_json())

        assert [type(result) for result in read_back.results] == [RunSuccess, RunFailure, RunSkip]
        assert read_back == summary

    def test_a_run_gives_a_skip_no_test_results(self, tmp_path: Path):
        """clair.run() adds the test results to each result that ran, and not to a skip."""
        project_root = write_project_marker(tmp_path / "project")
        for relative_path, text in {
            "db/s/upstream.py": 'from clair import Table\n\ntrouve = Table(sql="select 1 as id")\n',
            "db/s/downstream.py": (
                "from db.s.upstream import trouve as upstream\n\n"
                "from clair import Table\n\n"
                'trouve = Table(sql=f"select id from {upstream}")\n'
            ),
        }.items():
            (project_root / relative_path).parent.mkdir(parents=True, exist_ok=True)
            (project_root / relative_path).write_text(text)

        summary = clair.run(
            project_root,
            env=make_environment(),
            adapter=RecordingAdapter(fail_on=["db.s.upstream"]),
        )

        skip = summary.result("db.s.downstream")
        assert isinstance(skip, RunSkip)
        # model_copy(update=...) would add the attribute, and model_dump() would hide it.
        assert not hasattr(skip, "test_results")
        assert summary.test_results == []
