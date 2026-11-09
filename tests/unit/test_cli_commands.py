"""The tests of the CLI commands.

Each command parses the arguments, calls one function of `clair.api`, and sets
the status code. These tests give a false API function. They therefore prove the
two things that belong to the CLI: the arguments that it gives to the API, and
the status code that it gives back to the shell.

The integration tests run the API against Snowflake. The API tests in
`test_api.py` run it against a false adapter.
"""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
import structlog
from click.testing import CliRunner

from clair.cli.main import cli
from clair.core.runner import RunResult, RunStatus, RunSummary
from clair.core.test_runner import TestSummary
from clair.environments.environments import Authenticator, Environment, Password, PrivateKey, load_environment
from clair.exceptions import ClairError, EnvironmentsFileNotFoundError
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS
from tests.helpers import (
    make_environment,
    make_run_failure,
    make_run_skip,
    make_run_success,
    make_test_result,
)


@pytest.fixture(autouse=True)
def working_directory_is_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run each command inside a project. The commands find the root there."""
    monkeypatch.chdir(tmp_path)


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
    """Make one result of a run. A success and a failure hold one data quality test result."""
    address = "mydb.analytics.orders"
    if status == RunStatus.SKIPPED:
        return make_run_skip(address, skipped_by="mydb.analytics.upstream")
    test_results = [
        make_test_result(
            address,
            test_type="not_null",
            column_name="id",
            row_count=0 if tests_passed else 3,
        )
    ]
    if status == RunStatus.FAILURE:
        return make_run_failure(address, error="the statement failed", test_results=test_results)
    return make_run_success(address, test_results=test_results)


@pytest.fixture(autouse=True)
def environments_file(monkeypatch: pytest.MonkeyPatch) -> Environment:
    """Give the CLI one environment, and let it read no file.

    The CLI parses ~/.clair/environments.yml, and it gives the Environment to
    the API function. These tests replace the parse step only.
    """
    environment = make_environment()

    def fake_load(env_name: str) -> Environment:
        return environment.model_copy(update={"name": env_name})

    monkeypatch.setattr("clair.cli.main.load_environment", fake_load)
    return environment


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

    def fake(project_dir: str | Path | None = None, **keyword_arguments: Any) -> Any:
        calls["project_dir"] = project_dir
        calls.update(keyword_arguments)
        if error is not None:
            raise error
        return result

    monkeypatch.setattr(f"clair.api.{name}", fake)


def _result_of_command(command_name: str) -> RunSummary | TestSummary | None:
    """Give a result that the API function of *command_name* can return."""
    if command_name == "run":
        return RunSummary(results=[], env_name="dev", run_id="abc")
    if command_name == "test":
        return TestSummary(results=[])
    return None


class TestTheEnvironmentOption:
    """Each command that routes names its environment. clair selects no default."""

    @pytest.mark.parametrize("command_name", ["compile", "validate", "run", "test"])
    def test_a_command_without_an_environment_stops(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], command_name: str
    ):
        monkeypatch.delenv("CLAIR_ENV", raising=False)
        _give_api(monkeypatch, calls, command_name)

        result = CliRunner().invoke(cli, [command_name])

        assert result.exit_code == 2
        assert "--env" in result.output
        assert calls == {}

    @pytest.mark.parametrize("command_name", ["compile", "run", "test"])
    def test_clair_env_gives_the_environment(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], command_name: str
    ):
        monkeypatch.setenv("CLAIR_ENV", "prod")
        _give_api(monkeypatch, calls, command_name, _result_of_command(command_name))

        CliRunner().invoke(cli, [command_name])

        assert calls["env"].name == "prod"

    @pytest.mark.parametrize("command_name", ["compile", "run", "test"])
    def test_the_option_wins_against_clair_env(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], command_name: str
    ):
        monkeypatch.setenv("CLAIR_ENV", "prod")
        _give_api(monkeypatch, calls, command_name, _result_of_command(command_name))

        CliRunner().invoke(cli, [command_name, "--env", "dev"])

        assert calls["env"].name == "dev"


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

        result = CliRunner().invoke(cli, ["run", "--env", "dev"])

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

        result = CliRunner().invoke(cli, ["run", "--env", "dev"])

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

        result = CliRunner().invoke(cli, ["run", "--env", "dev"])

        assert result.exit_code == 1

    def test_a_run_with_no_trouve_says_so(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch, calls, "run", RunSummary(results=[], env_name="dev", run_id="abc")
        )

        result = CliRunner().invoke(cli, ["run", "--env", "dev"])

        assert result.exit_code == 0
        assert "no Trouves to run" in result.output

    def test_an_error_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "run", error=ClairError("no environments.yml"))

        result = CliRunner().invoke(cli, ["run", "--env", "dev"])

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
                "--select", "+mydb.analytics.orders",
                "--exclude", "mydb.reports.*",
                "--env", "prod",
                "--run-mode", "incremental",
                "--no-test",
                "--sample",
                "--sample-rows", "250",
                "--threads", "3",
            ],
        )

        # The CLI passes no directory. The API walks up from the working
        # directory to the project root.
        assert calls["project_dir"] is None
        assert calls["select"] == ("+mydb.analytics.orders",)
        assert calls["exclude"] == ("mydb.reports.*",)
        assert isinstance(calls["env"], Environment)
        assert calls["env"].name == "prod"
        assert calls["run_mode"] == RunMode.INCREMENTAL
        assert calls["test"] is False
        assert calls["sample"] is True
        assert calls["sample_rows"] == 250
        assert calls["threads"] == 3

    @pytest.mark.parametrize("command_name", ["compile", "run", "test"])
    def test_sample_rows_has_a_default(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], command_name: str
    ):
        _give_api(monkeypatch, calls, command_name, _result_of_command(command_name))

        CliRunner().invoke(cli, [command_name, "--env", "dev", "--sample"])

        assert calls["sample"] is True
        assert calls["sample_rows"] == DEFAULT_SAMPLE_ROWS

    @pytest.mark.parametrize(
        "arguments",
        [
            ["compile", "--env", "dev", "--sample-rows", "10"],
            ["run", "--env", "dev", "--sample-rows", "10"],
            ["test", "--env", "dev", "--sample-rows", "10"],
            ["run", "--env", "dev", "--sample", "--sample-rows", "0"],
        ],
    )
    def test_a_bad_sample_rows_stops_the_command(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], arguments: list[str]
    ):
        _give_api(monkeypatch, calls, arguments[0], RunSummary(results=[], env_name="dev", run_id="abc"))

        result = CliRunner().invoke(cli, arguments)

        assert result.exit_code == 2
        assert calls == {}


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
                    make_test_result(
                        "mydb.analytics.orders",
                        test_type="unique",
                        column_name="id",
                        row_count=2,
                    )
                ]
            ),
        )

        result = CliRunner().invoke(cli, ["test", "--env", "dev"])

        assert result.exit_code == 1

    def test_a_test_that_passes_gives_the_status_code_0(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "test", TestSummary(results=[]))

        result = CliRunner().invoke(cli, ["test", "--env", "dev"])

        assert result.exit_code == 0

    def test_the_command_gives_each_option_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "test", TestSummary(results=[]))

        CliRunner().invoke(
            cli,
            [
                "test",
                "--env", "dev",
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

        result = CliRunner().invoke(cli, ["compile", "--env", "dev"])

        assert result.exit_code == 1

    def test_the_command_gives_the_run_mode_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "compile", result=None)

        result = CliRunner().invoke(
            cli, ["compile", "--env", "dev", "--run-mode", "incremental"]
        )

        assert result.exit_code == 0
        assert calls["run_mode"] == RunMode.INCREMENTAL

    def test_the_command_gives_the_parsed_environment_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "compile", result=None)

        CliRunner().invoke(cli, ["compile", "--env", "prod"])

        assert isinstance(calls["env"], Environment)
        assert calls["env"].name == "prod"

    def test_the_command_continues_without_environments_yml(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        """A compile makes no connection, thus the name alone is enough."""

        def fake_load(env_name: str) -> Environment:
            raise EnvironmentsFileNotFoundError("/home/analyst/.clair/environments.yml")

        monkeypatch.setattr("clair.cli.main.load_environment", fake_load)
        _give_api(monkeypatch, calls, "compile", result=None)

        result = CliRunner().invoke(
            cli, ["compile", "--env", "prod"]
        )

        assert result.exit_code == 0
        assert calls["env"] == "prod"


class TestDocsCommand:
    def test_a_port_that_is_in_use_gives_the_status_code_1(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(
            monkeypatch, calls, "docs", error=OSError("[Errno 48] Address already in use")
        )

        result = CliRunner().invoke(cli, ["docs"])

        assert result.exit_code == 1

    def test_the_command_gives_each_option_to_the_api(
        self, monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], tmp_path: Path
    ):
        _give_api(monkeypatch, calls, "docs", result=None)

        CliRunner().invoke(
            cli,
            [
                "docs",
                "--port", "9000",
                "--host", "0.0.0.0",
                "--no-browser",
            ],
        )

        assert calls["port"] == 9000
        assert calls["host"] == "0.0.0.0"
        assert calls["open_browser"] is False


class TestVersion:
    """`clair --version` must print the version of the installed package."""

    def test_the_option_prints_the_version_of_the_package(self):
        result = CliRunner().invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert result.output.strip() == f"clair, version {version('rivage-clair')}"


class TestInitWritesAnEnvironmentThatLoads:
    """`clair init` asks for the settings, and each run reads the file that it writes."""

    # The answers before the authentication method, and the answers after it.
    ACCOUNT_ANSWERS = ("dev", "myorg-myaccount", "me@example.com")
    SESSION_ANSWERS = ("COMPUTE_WH", "", "us-east-1", "abc12345", "4", "source.orders.raw")

    @pytest.mark.parametrize(
        ("method_answers", "expected_authentication"),
        [
            (["1", "~/.clair/key.p8", "n"], PrivateKey(private_key_path="~/.clair/key.p8")),
            (["2", "secret"], Password(password="secret")),
            (["3"], Authenticator(authenticator="externalbrowser")),
        ],
    )
    def test_each_method_gives_its_authentication(
        self,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        method_answers: list[str],
        expected_authentication,
    ):
        home_dir = tmp_path_factory.mktemp("home")
        monkeypatch.setenv("HOME", str(home_dir))
        project_dir = tmp_path_factory.mktemp("init") / "proj"
        answers = [*self.ACCOUNT_ANSWERS, *method_answers, *self.SESSION_ANSWERS]

        result = CliRunner().invoke(
            cli, ["init", "--project", str(project_dir)], input="\n".join(answers) + "\n"
        )

        assert result.exit_code == 0, result.output
        environment = load_environment(
            "dev", environments_path=home_dir / ".clair" / "environments.yml"
        )
        assert environment.authentication == expected_authentication
        assert environment.connection.role is None
        assert environment.connection.account_locator == "abc12345"
