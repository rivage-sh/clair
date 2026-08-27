"""The tests of the CLI commands.

Each command parses the arguments, calls one function of `clair.api`, and sets
the status code. These tests give a false API function. They therefore prove the
two things that belong to the CLI: the arguments that it gives to the API, and
the status code that it gives back to the shell.

The integration tests run the API against Snowflake. The API tests in
`test_api.py` run it against a false adapter.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import structlog
from click.testing import CliRunner

from clair.cli.main import cli
from clair.core.runner import RunResult, RunStatus, RunSummary
from clair.core.test_runner import TestResult, TestSummary
from clair.exceptions import ClairError
from clair.trouves.run_config import RunMode


@pytest.fixture(autouse=True)
def restore_the_logger() -> Any:
    """Give structlog its default configuration back after each command.

    `cli()` configures structlog, and CliRunner gives it a temporary stdout.
    That file closes when the command ends. A later test would then write to a
    closed file.
    """
    yield
    structlog.reset_defaults()


def _run_result(
    status: RunStatus = RunStatus.SUCCESS, tests_passed: bool = True
) -> RunResult:
    """Make one result of a run, with one data quality test result."""
    return RunResult(
        logical_address="mydb.analytics.orders",
        physical_address="mydb.analytics.orders",
        status=status,
        skipped_by="mydb.analytics.upstream" if status == RunStatus.SKIPPED else None,
        test_results=[
            TestResult(
                physical_address="mydb.analytics.orders",
                test_index=0,
                test_type="not_null",
                column_name="id",
                passed=tests_passed,
                failing_row_count=0 if tests_passed else 3,
            )
        ],
    )


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Record the arguments of the API function that the command calls."""
    return {}


def _give_api(
    monkeypatch: pytest.MonkeyPatch,
    calls: dict[str, Any],
    name: str,
    result: Any = None,
    error: Exception | None = None,
) -> None:
    """Replace one function of `clair.api` with a false function."""

    def fake(project_dir: str | Path = ".", **keyword_arguments: Any) -> Any:
        calls["project_dir"] = project_dir
        calls.update(keyword_arguments)
        if error is not None:
            raise error
        return result

    monkeypatch.setattr(f"clair.api.{name}", fake)


class TestRunCommand:
    def test_a_run_that_passes_gives_the_status_code_0(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch,
            calls,
            "run",
            RunSummary(results=[_run_result()], env_name="dev", run_id="abc"),
        )

        result = CliRunner().invoke(cli, ["run", "--project", str(tmp_path)])

        assert result.exit_code == 0

    def test_a_trouve_that_fails_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch,
            calls,
            "run",
            RunSummary(
                results=[_run_result(status=RunStatus.FAILURE)],
                env_name="dev",
                run_id="abc",
            ),
        )

        result = CliRunner().invoke(cli, ["run", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_a_data_quality_test_that_fails_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        """The Trouve built, and its test rejected the data. That is a failure."""
        _give_api(
            monkeypatch,
            calls,
            "run",
            RunSummary(
                results=[_run_result(tests_passed=False)], env_name="dev", run_id="abc"
            ),
        )

        result = CliRunner().invoke(cli, ["run", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_a_run_with_no_trouve_says_so(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch, calls, "run", RunSummary(results=[], env_name="dev", run_id="abc")
        )

        result = CliRunner().invoke(cli, ["run", "--project", str(tmp_path)])

        assert result.exit_code == 0
        assert "no Trouves to run" in result.output

    def test_an_error_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "run", error=ClairError("no environments.yml"))

        result = CliRunner().invoke(cli, ["run", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_the_command_gives_each_option_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch, calls, "run", RunSummary(results=[], env_name="dev", run_id="abc")
        )

        CliRunner().invoke(
            cli,
            [
                "run",
                "--project", str(tmp_path),
                "--select", "+mydb.analytics.orders",
                "--exclude", "mydb.reports.*",
                "--env", "prod",
                "--run-mode", "incremental",
                "--no-test",
                "--sample",
                "--threads", "3",
            ],
        )

        assert calls["project_dir"] == str(tmp_path)
        assert calls["select"] == ("+mydb.analytics.orders",)
        assert calls["exclude"] == ("mydb.reports.*",)
        assert calls["env"] == "prod"
        assert calls["run_mode"] == RunMode.INCREMENTAL
        assert calls["test"] is False
        assert calls["sample"] is True
        assert calls["threads"] == 3


class TestTestCommand:
    def test_a_test_that_fails_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch,
            calls,
            "test",
            TestSummary(
                results=[
                    TestResult(
                        physical_address="mydb.analytics.orders",
                        test_index=0,
                        test_type="unique",
                        column_name="id",
                        passed=False,
                        failing_row_count=2,
                    )
                ]
            ),
        )

        result = CliRunner().invoke(cli, ["test", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_a_test_that_passes_gives_the_status_code_0(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "test", TestSummary(results=[]))

        result = CliRunner().invoke(cli, ["test", "--project", str(tmp_path)])

        assert result.exit_code == 0

    def test_the_command_gives_each_option_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "test", TestSummary(results=[]))

        CliRunner().invoke(
            cli,
            [
                "test",
                "--project", str(tmp_path),
                "--select", "mydb.*",
                "--sample",
                "--threads", "2",
            ],
        )

        assert calls["select"] == ("mydb.*",)
        assert calls["sample"] is True
        assert calls["threads"] == 2


class TestCompileCommand:
    def test_an_error_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "compile", error=ClairError("a bad Trouve"))

        result = CliRunner().invoke(cli, ["compile", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_the_command_gives_the_run_mode_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "compile", result=None)

        result = CliRunner().invoke(
            cli, ["compile", "--project", str(tmp_path), "--run-mode", "incremental"]
        )

        assert result.exit_code == 0
        assert calls["run_mode"] == RunMode.INCREMENTAL


class TestDocsCommand:
    def test_a_port_that_is_in_use_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch, calls, "docs", error=OSError("[Errno 48] Address already in use")
        )

        result = CliRunner().invoke(cli, ["docs", "--project", str(tmp_path)])

        assert result.exit_code == 1

    def test_the_command_gives_each_option_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "docs", result=None)

        CliRunner().invoke(
            cli,
            [
                "docs",
                "--project", str(tmp_path),
                "--port", "9000",
                "--host", "0.0.0.0",
                "--no-browser",
            ],
        )

        assert calls["port"] == 9000
        assert calls["host"] == "0.0.0.0"
        assert calls["open_browser"] is False
