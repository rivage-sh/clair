"""A run that is cancelled stops cleanly: it promotes nothing more, and its summary stays complete.

The first SIGINT or SIGTERM cancels the run. clair starts no new Trouve, asks
each connection to cancel its statement, sends no new statement, and promotes
nothing. Each Trouve that did not start gets the status CANCELLED, thus a retry
selects it. The second signal ends the process at once.
"""

from __future__ import annotations

import os
import signal
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self, cast

import pytest
from click.testing import CliRunner

import clair
from clair.adapters.base import Statement, StatementStatus
from clair.adapters.snowflake import SnowflakeAdapter
from clair.cli.main import cli
from clair.core.artifacts import write_run_result
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.core.runner import (
    RUN_CANCELLED_ERROR,
    RunCancel,
    RunCancellation,
    RunFailure,
    RunResult,
    RunSkip,
    RunSuccess,
    RunSummary,
    run_project,
)
from tests.helpers import RecordingAdapter, addresses_of_text, make_environment, write_project_marker

FIRST = "db.derived.first"
DOWNSTREAM = "db.derived.downstream"
ALONE = "db.derived.alone"


def _write_project(project_root: Path) -> Path:
    """Write first -> downstream, and alone."""
    write_project_marker(project_root)
    (project_root / "db" / "derived").mkdir(parents=True)
    (project_root / "db" / "derived" / "first.py").write_text(
        'from clair import Table, TestUnique\ntrouve = Table(sql="select 1 as id", tests=[TestUnique(column="id")])\n'
    )
    (project_root / "db" / "derived" / "downstream.py").write_text(
        "from clair import Table\n"
        "from db.derived.first import trouve as first\n"
        'trouve = Table(sql=f"select * from {first}")\n'
    )
    (project_root / "db" / "derived" / "alone.py").write_text(
        'from clair import Table\ntrouve = Table(sql="select 2 as id")\n'
    )
    return project_root


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    return _write_project(tmp_path / "project")


def _promotions(statements: list[str]) -> list[str]:
    return [sql for sql in statements if "promote the tested" in sql]


class _BlockingAdapter(RecordingAdapter):
    """An adapter whose first build runs until a cancel arrives, as a long Snowflake query does."""

    def __init__(self, **fields: Any) -> None:
        super().__init__(**fields)
        self.build_started = threading.Event()
        self.blocked_address: str | None = None
        self._cancelled = threading.Event()
        self.cancel_count = 0

    def execute(self, sql: str) -> Statement:
        if sql.startswith("CREATE OR REPLACE TABLE") and self.blocked_address is None:
            self.blocked_address = sql.split()[4].split("__clair_")[0]
            self.record.enter(sql)
            self.record.leave()
            self.build_started.set()
            assert self._cancelled.wait(timeout=5)
            return Statement(sql=sql, status=StatementStatus.FAILURE, error="SQL execution canceled")
        return super().execute(sql)

    def cancel(self) -> None:
        self.cancel_count += 1
        self._cancelled.set()


def test_a_cancel_before_the_first_trouve_cancels_each_trouve(project_root):
    adapter = RecordingAdapter()
    run_handle = clair.start_run(project_root, env=make_environment(), adapter=adapter, threads=1)

    run_handle.cancel()
    results = list(run_handle)

    assert all(isinstance(result, RunCancel) for result in results)
    assert len(results) == 3
    assert not any(sql.startswith("CREATE OR REPLACE TABLE") for sql in adapter.record.statements)


def test_a_cancel_during_a_build_stops_the_statement_and_promotes_nothing(project_root):
    adapter = _BlockingAdapter()
    run_handle = clair.start_run(project_root, env=make_environment(), adapter=adapter, threads=1)

    def cancel_when_the_build_runs() -> None:
        assert adapter.build_started.wait(timeout=5)
        run_handle.cancel()

    canceller = threading.Thread(target=cancel_when_the_build_runs)
    canceller.start()
    results = {str(result.addresses.logical): result for result in run_handle}
    canceller.join()

    # The Trouve whose build ran fails, and keeps its data. Each other Trouve
    # did not start: it waits for that Trouve, or the cancel came first.
    assert adapter.cancel_count == 1
    assert adapter.blocked_address is not None
    assert isinstance(results.pop(adapter.blocked_address), RunFailure)
    assert all(isinstance(result, RunCancel | RunSkip) for result in results.values())
    assert _promotions(adapter.record.statements) == []
    assert run_handle.summary.cancelled_count >= 1


def _run_directly(project_root: Path, adapter: RecordingAdapter, cancellation: RunCancellation, **options: Any) -> list[RunResult]:
    trouves = discover_project(project_root)
    from clair.core.references import recompile_for_selection

    dag = build_dag(trouves)
    selected = get_executable_nodes(dag)
    recompile_for_selection(trouves, set(selected))
    return list(
        run_project(dag, selected, adapter, run_id="0" * 32, cancellation=cancellation, threads=1, **options)
    )


def test_a_cancel_after_the_build_keeps_the_candidate_and_does_not_promote(project_root):
    # The cancel arrives while the tests of FIRST run: the tests pass, and
    # clair must still stop before the promotion.
    adapter = RecordingAdapter()
    cancellation = RunCancellation()

    def tests_that_see_a_cancel(name: str, address: str, _adapter: object) -> bool:
        if name == FIRST:
            cancellation.cancel()
        return True

    results = _run_directly(
        project_root, adapter, cancellation, after_node_success=tests_that_see_a_cancel, use_staging=True
    )

    (first,) = [result for result in results if str(result.addresses.logical) == FIRST]
    assert isinstance(first, RunFailure)
    assert "cancelled before the promotion" in first.error
    assert not any(FIRST in sql for sql in _promotions(adapter.record.statements))
    assert not any(
        sql.startswith("-- staging: drop") and FIRST in sql for sql in adapter.record.statements
    )


def test_a_cancel_between_two_statements_sends_no_more_statements(project_root):
    # A run with no staging writes the physical table. After the cancel, the
    # comments and each later statement of the plan stay unsent.
    (project_root / "db" / "derived" / "first.py").write_text(
        'from clair import Table\ntrouve = Table(sql="select 1 as id", docs="The first table.")\n'
    )
    cancellation = RunCancellation()

    class _CancelAfterTheBuild(RecordingAdapter):
        def execute(self, sql: str) -> Statement:
            statement = super().execute(sql)
            if sql.startswith(f"CREATE OR REPLACE TABLE {FIRST}"):
                cancellation.cancel()
            return statement

    adapter = _CancelAfterTheBuild()
    results = _run_directly(project_root, adapter, cancellation, use_staging=False)

    (first,) = [result for result in results if str(result.addresses.logical) == FIRST]
    assert isinstance(first, RunFailure)
    assert first.failed_statement is not None
    assert first.failed_statement.error == RUN_CANCELLED_ERROR
    assert not any("COMMENT" in sql for sql in adapter.record.statements)


def test_a_cancelled_result_survives_the_summary_file_and_writes_no_artifact(tmp_path):
    cancelled = RunCancel(addresses=addresses_of_text(ALONE))
    summary = RunSummary(results=[cancelled], env_name="prod", run_id="0" * 32)

    assert RunSummary.model_validate_json(summary.model_dump_json()).results == [cancelled]
    assert "CANCELLED" in summary.render()
    assert write_run_result(tmp_path, summary.run_id, cancelled) is None


class _WaitingCursor:
    """A cursor whose statement runs until the connector receives the cancel."""

    def __init__(self, connection: _CancellingConnection) -> None:
        self._connection = connection
        self.sfqid = "query-1"
        self.rowcount = 0

    def execute(self, sql: str, _statement_params: dict[str, str] | None = None) -> None:
        assert _statement_params is not None
        self._connection.request_ids.append(_statement_params["requestId"])
        self._connection.running.set()
        assert self._connection.cancelled.wait(timeout=5)
        raise RuntimeError("SQL execution canceled")

    def close(self) -> None:
        pass


class _CancellingConnection:
    def __init__(self) -> None:
        self.running = threading.Event()
        self.cancelled = threading.Event()
        self.request_ids: list[str] = []
        self.cancel_calls: list[tuple[str, str]] = []

    def cursor(self) -> _WaitingCursor:
        return _WaitingCursor(self)

    def _cancel_query(self, sql: str, request_id: object) -> None:
        self.cancel_calls.append((sql, str(request_id)))
        self.cancelled.set()


def test_the_snowflake_adapter_cancels_the_statement_that_it_waits_for():
    connection = _CancellingConnection()
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    SnowflakeAdapter.__init__(adapter)
    adapter._conn = cast(Any, connection)
    adapter._environment = make_environment()

    adapter.cancel()  # Nothing runs, thus nothing to cancel.
    assert connection.cancel_calls == []

    results: list[Statement] = []
    worker = threading.Thread(target=lambda: results.append(adapter.execute("\n  SELECT SYSTEM$WAIT(60)\n")))
    worker.start()
    assert connection.running.wait(timeout=5)
    adapter.cancel()
    worker.join(timeout=5)

    # The connector names the statement by the text that it sends, and by the request id.
    assert connection.cancel_calls == [("SELECT SYSTEM$WAIT(60)", connection.request_ids[0])]
    (statement,) = results
    assert not statement.success


class _SignalledRunHandle:
    """A run handle that receives signals while it gives its results."""

    def __init__(self, signal_number: int, signal_count: int) -> None:
        self.run_id = "0" * 32
        self.cancel_count = 0
        self._signal_number = signal_number
        self._signal_count = signal_count
        self._results: list[RunResult] = [
            RunSuccess(addresses=addresses_of_text(FIRST), statements=[], effective_run_mode="full_refresh", duration_seconds=0, row_count=0),
            RunCancel(addresses=addresses_of_text(ALONE)),
        ]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exception_info: object) -> None:
        pass

    def cancel(self) -> None:
        self.cancel_count += 1

    def __iter__(self) -> Iterator[RunResult]:
        for _ in range(self._signal_count):
            os.kill(os.getpid(), self._signal_number)
        yield from self._results

    @property
    def summary(self) -> RunSummary:
        return RunSummary(results=self._results, env_name="prod", run_id=self.run_id)


@pytest.fixture
def cli_project(project_root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(project_root)
    monkeypatch.setattr("clair.cli.main._load_environment", lambda name: make_environment(name))
    return project_root


@pytest.mark.parametrize("signal_number", [signal.SIGINT, signal.SIGTERM])
def test_the_first_signal_cancels_the_run_and_writes_the_summary(cli_project, monkeypatch, signal_number):
    run_handle = _SignalledRunHandle(signal_number, signal_count=1)
    monkeypatch.setattr("clair.api.start_run", lambda *_, **__: run_handle)
    handler_before_the_command = signal.getsignal(signal_number)

    result = CliRunner().invoke(cli, ["run", "--env", "prod"])

    assert run_handle.cancel_count == 1
    assert result.exit_code == 128 + signal_number
    assert "clair stops" in result.output
    assert (cli_project / "_clairtifacts" / run_handle.run_id / "run_summary.json").is_file()
    # The command gives the handler back after the run.
    assert signal.getsignal(signal_number) == handler_before_the_command


def test_the_second_signal_ends_the_process_at_once(cli_project, monkeypatch):
    run_handle = _SignalledRunHandle(signal.SIGINT, signal_count=2)
    monkeypatch.setattr("clair.api.start_run", lambda *_, **__: run_handle)
    exit_codes: list[int] = []

    class _ProcessEnded(Exception):
        pass

    def fake_exit(code: int) -> None:
        exit_codes.append(code)
        raise _ProcessEnded

    monkeypatch.setattr(os, "_exit", fake_exit)

    result = CliRunner().invoke(cli, ["run", "--env", "prod"])

    assert isinstance(result.exception, _ProcessEnded)
    assert exit_codes == [128 + signal.SIGINT]
    assert run_handle.cancel_count == 1
    assert not (cli_project / "_clairtifacts" / run_handle.run_id / "run_summary.json").exists()
