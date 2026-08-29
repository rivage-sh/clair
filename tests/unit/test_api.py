"""The tests of the Python API. They use RecordingAdapter and a false environment."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import clair
from clair.core.runner import RunStatus
from clair.environments.environments import Environment
from clair.exceptions import ResultNotFoundError
from clair.trouves.run_config import RunMode
from tests.helpers import RecordingAdapter

DAILY_ORDERS = "analytics.revenue.daily_orders"


@pytest.fixture
def project(simple_project: Path, tmp_path: Path) -> Path:
    """Copy the simple project, thus a compile writes to a temporary directory."""
    destination = tmp_path / "simple_project"
    shutil.copytree(simple_project, destination)

    # The fixture project has no data quality test. The API tests need one, thus
    # a run can show the test results of a Trouve.
    trouve_file = destination / "analytics" / "revenue" / "daily_orders.py"
    source_text = trouve_file.read_text()
    source_text = source_text.replace(
        "from clair import Column, ColumnType, Trouve, TrouveType",
        "from clair import Column, ColumnType, TestNotNull, Trouve, TrouveType",
    ).replace(
        "    columns=[",
        "    tests=[TestNotNull(column=\"order_date\")],\n    columns=[",
    )
    trouve_file.write_text(source_text)
    return destination


@pytest.fixture
def environment() -> Environment:
    """Give the parsed environment that each operation accepts.

    The API functions read no file, thus the test makes the object. A notebook
    or a service does the same thing.
    """
    return Environment(
        name="dev",
        account="test-account",
        user="test-user",
        warehouse="test_wh",
        role="test_role",
        account_locator="ab12345",
    )


class TestTheModuleGivesTheOperations:
    def test_each_operation_is_an_attribute_of_clair(self):
        """A user reaches the operations from the package, and imports no submodule."""
        for name in ("run", "compile", "test", "docs", "catalog"):
            assert callable(getattr(clair, name)), name


class TestCompile:
    def test_it_gives_the_sql_of_each_trouve(self, project: Path, environment: Environment):
        output = clair.compile(project, env=environment)

        assert output.trouve_count == 1
        assert output.source_count == 1
        assert output.env_name == "dev"
        assert output.run_id
        assert output.project_root == project

        node = output.node(DAILY_ORDERS)
        assert node is not None
        assert str(node.addresses.physical) == DAILY_ORDERS
        assert str(node.addresses.logical) == DAILY_ORDERS
        assert node.dependencies == ["source.raw.orders"]
        assert any("daily_orders" in statement for statement in node.sql)

    def test_the_staging_address_is_run_scoped(self, project: Path, environment: Environment):
        output = clair.compile(project, env=environment)
        node = output.node(DAILY_ORDERS)

        assert node is not None
        assert node.addresses.staging is not None
        assert output.run_id[:8] in str(node.addresses.staging)

    def test_use_staging_false_removes_the_staging_address(
        self, project: Path, environment: Environment
    ):
        node = clair.compile(project, env=environment, use_staging=False).node(DAILY_ORDERS)

        assert node is not None
        assert node.addresses.staging is None

    def test_it_writes_the_artifact_file(self, project: Path, environment: Environment):
        node = clair.compile(project, env=environment).node(DAILY_ORDERS)

        assert node is not None
        assert node.artifact_path is not None
        assert node.artifact_path.read_text()

    def test_exclude_removes_a_trouve(self, project: Path, environment: Environment):
        output = clair.compile(project, env=environment, exclude=[DAILY_ORDERS])

        assert output.compiled_nodes == []


class TestRun:
    def test_it_gives_the_result_of_each_trouve(self, project: Path, environment: Environment):
        summary = clair.run(project, env=environment, adapter=RecordingAdapter(), test=False)

        assert summary.succeeded_count == 1
        assert summary.failed_count == 0
        assert summary.env_name == "dev"
        assert summary.run_id
        assert summary.run_mode == RunMode.FULL_REFRESH

        result = summary.result(DAILY_ORDERS)
        assert result is not None
        assert result.status == RunStatus.SUCCESS
        assert [s for s in result.statements if s.query_id]
        assert result.row_count == 42

    def test_the_result_holds_the_sql_that_clair_executed(
        self, project: Path, environment: Environment
    ):
        """A caller reads the statements, and parses no log line."""
        result = clair.run(project, env=environment, adapter=RecordingAdapter(), test=False).result(DAILY_ORDERS)

        assert result is not None
        assert result.statements
        assert any("daily_orders" in s.sql for s in result.statements)

    def test_a_run_without_the_tests_writes_to_the_physical_address(
        self, project: Path, environment: Environment
    ):
        result = clair.run(project, env=environment, adapter=RecordingAdapter(), test=False).result(DAILY_ORDERS)

        assert result is not None
        assert result.addresses.staging is None
        assert result.effective_run_mode == RunMode.FULL_REFRESH

    def test_a_run_with_the_tests_writes_to_a_staging_address(
        self, project: Path, environment: Environment
    ):
        summary = clair.run(project, env=environment, adapter=RecordingAdapter())
        result = summary.result(DAILY_ORDERS)

        assert result is not None
        assert result.status == RunStatus.SUCCESS
        assert result.addresses.staging is not None
        assert summary.run_id[:8] in str(result.addresses.staging)

    def test_the_result_holds_the_test_results(self, project: Path, environment: Environment):
        summary = clair.run(project, env=environment, adapter=RecordingAdapter())
        result = summary.result(DAILY_ORDERS)

        assert result is not None
        assert result.test_results
        assert all(test_result.passed for test_result in result.test_results)
        assert summary.test_results == result.test_results

    def test_a_failure_gives_the_error_and_the_sql(self, project: Path, environment: Environment):
        summary = clair.run(
            project,
            env=environment,
            adapter=RecordingAdapter(fail_on=["daily_orders"]),
            test=False,
        )

        assert summary.failed_count == 1
        result = summary.failed[0]
        assert "Simulated failure" in result.error
        assert result.statements
        assert result.failed_statement is not None

    def test_a_selection_that_matches_nothing_gives_an_empty_summary(
        self, project: Path, environment: Environment
    ):
        summary = clair.run(project, env=environment, select=["no.such.trouve"], adapter=RecordingAdapter())

        assert summary.results == []
        assert summary.succeeded_count == 0

    def test_more_than_one_thread_opens_more_connections(
        self, project: Path, environment: Environment
    ):
        """A parallel run gives each thread a private connection."""
        adapter = RecordingAdapter()
        summary = clair.run(project, env=environment, adapter=adapter, test=False, threads=2)

        assert summary.succeeded_count == 1
        # One Trouve runs, thus clair limits the pool to one connection, and it
        # opens no second connection.
        assert adapter.record.adapter_count == 1

    def test_it_does_not_close_an_adapter_that_the_caller_gave(
        self, project: Path, environment: Environment
    ):
        """A notebook keeps one connection open for many calls."""
        adapter = RecordingAdapter()
        clair.run(project, env=environment, adapter=adapter, test=False)

        assert adapter.is_open is True


    def test_an_unknown_address_raises(self, project: Path, environment: Environment):
        """An address that no result holds is a fault of the caller."""
        summary = clair.run(
            project, env=environment, adapter=RecordingAdapter(), test=False
        )

        with pytest.raises(ResultNotFoundError, match="no result for"):
            summary.result("no.such.trouve")


class TestTest:
    def test_it_gives_one_result_for_each_test(self, project: Path, environment: Environment):
        summary = clair.test(project, env=environment, adapter=RecordingAdapter())

        assert summary.results
        assert summary.failed_count == 0
        assert summary.error_count == 0

    def test_a_selection_that_matches_nothing_gives_an_empty_summary(
        self, project: Path, environment: Environment
    ):
        summary = clair.test(project, env=environment, select=["no.such.trouve"], adapter=RecordingAdapter())

        assert summary.results == []


class TestTheEnvironmentArgument:
    """The operations accept the parsed object, and they read no file."""

    def test_compile_accepts_a_name(self, project: Path):
        """A compile makes no connection, thus the name is enough."""
        assert clair.compile(project, env="prod").env_name == "prod"

    def test_compile_without_an_environment_gives_dev(self, project: Path):
        assert clair.compile(project).env_name == "dev"

    def test_run_needs_the_environment(self, project: Path):
        """A run connects to the warehouse, thus a name is not enough."""
        with pytest.raises(TypeError):
            clair.run(project, adapter=RecordingAdapter())  # ty: ignore[missing-argument]


class TestCatalog:
    def test_it_needs_no_connection(self, project: Path):
        project_catalog = clair.catalog(project)

        assert project_catalog["trouves"]
