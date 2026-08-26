"""The tests of the Python API. They use a false adapter and a false environment."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import clair
from clair.adapters.base import QueryResult, WarehouseAdapter
from clair.core.runner import RunStatus
from clair.environments.environments import Environment
from clair.trouves.run_config import RunMode

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
def fake_environment(monkeypatch: pytest.MonkeyPatch) -> Environment:
    """Make clair.api.load_environment give one environment, and read no file."""
    environment = Environment(
        name="dev",
        account="test-account",
        user="test-user",
        warehouse="test_wh",
        role="test_role",
        account_locator="ab12345",
    )
    monkeypatch.setattr(
        "clair.api.load_environment", lambda env_name=None: ("dev", environment)
    )
    return environment


def _make_adapter(fail_on: set[str] | None = None) -> MagicMock:
    """Make a false adapter. It fails on a statement that names a fail_on value."""
    fail_on = fail_on or set()
    adapter = MagicMock(spec=WarehouseAdapter)
    call_count = 0

    def execute(sql: str) -> QueryResult:
        nonlocal call_count
        call_count += 1
        query_id = f"qid-{call_count:04d}"
        for name in fail_on:
            if name in sql:
                return QueryResult(
                    query_id=query_id,
                    query_url=f"https://test.snowflake.com/#/query/{query_id}",
                    success=False,
                    error=f"Simulated failure for {name}",
                )
        # A test query gives the rows that disobey the test. Zero rows is a
        # test that passes. A build statement gives the rows that it wrote.
        is_test_query = sql.lstrip().upper().startswith("SELECT")
        return QueryResult(
            query_id=query_id,
            query_url=f"https://test.snowflake.com/#/query/{query_id}",
            success=True,
            row_count=0 if is_test_query else 42,
        )

    adapter.execute.side_effect = execute
    adapter.table_exists.return_value = True
    adapter.fetch_dataframe.return_value = None
    return adapter


class TestTheModuleGivesTheOperations:
    def test_each_operation_is_an_attribute_of_clair(self):
        """A user reaches the operations from the package, and imports no submodule."""
        for name in ("run", "compile", "test", "catalog", "serve_docs"):
            assert callable(getattr(clair, name)), name

    def test_an_unknown_attribute_raises(self):
        with pytest.raises(AttributeError):
            clair.no_such_operation  # noqa: B018 -- the attribute access is the test


class TestCompile:
    def test_it_gives_the_sql_of_each_trouve(self, project: Path, fake_environment):
        output = clair.compile(project)

        assert output.trouve_count == 1
        assert output.source_count == 1
        assert output.env_name == "dev"
        assert output.run_id
        assert output.project_root == project

        node = output.node(DAILY_ORDERS)
        assert node is not None
        assert node.physical_address == DAILY_ORDERS
        assert node.logical_address == DAILY_ORDERS
        assert node.dependencies == ["source.raw.orders"]
        assert any("daily_orders" in statement for statement in node.sql)

    def test_the_staging_address_is_run_scoped(self, project: Path, fake_environment):
        output = clair.compile(project)
        node = output.node(DAILY_ORDERS)

        assert node is not None
        assert node.staging_address is not None
        assert output.run_id[:8] in node.staging_address

    def test_use_staging_false_removes_the_staging_address(
        self, project: Path, fake_environment
    ):
        node = clair.compile(project, use_staging=False).node(DAILY_ORDERS)

        assert node is not None
        assert node.staging_address is None

    def test_it_writes_the_artifact_file(self, project: Path, fake_environment):
        node = clair.compile(project).node(DAILY_ORDERS)

        assert node is not None
        assert node.artifact_path is not None
        assert node.artifact_path.read_text()

    def test_exclude_removes_a_trouve(self, project: Path, fake_environment):
        output = clair.compile(project, exclude=[DAILY_ORDERS])

        assert output.compiled_nodes == []


class TestRun:
    def test_it_gives_the_result_of_each_trouve(self, project: Path, fake_environment):
        summary = clair.run(project, adapter=_make_adapter(), test=False)

        assert summary.succeeded_count == 1
        assert summary.failed_count == 0
        assert summary.env_name == "dev"
        assert summary.run_id
        assert summary.run_mode == RunMode.FULL_REFRESH

        result = summary.result(DAILY_ORDERS)
        assert result is not None
        assert result.status == RunStatus.SUCCESS
        assert result.query_ids
        assert result.row_count == 42

    def test_the_result_holds_the_sql_that_clair_executed(
        self, project: Path, fake_environment
    ):
        """A caller reads the statements, and parses no log line."""
        result = clair.run(project, adapter=_make_adapter(), test=False).result(DAILY_ORDERS)

        assert result is not None
        assert result.sql
        assert any("daily_orders" in statement for statement in result.sql)

    def test_a_run_without_the_tests_writes_to_the_physical_address(
        self, project: Path, fake_environment
    ):
        result = clair.run(project, adapter=_make_adapter(), test=False).result(DAILY_ORDERS)

        assert result is not None
        assert result.staging_address is None
        assert result.effective_run_mode == RunMode.FULL_REFRESH

    def test_a_run_with_the_tests_writes_to_a_staging_address(
        self, project: Path, fake_environment
    ):
        summary = clair.run(project, adapter=_make_adapter())
        result = summary.result(DAILY_ORDERS)

        assert result is not None
        assert result.status == RunStatus.SUCCESS
        assert result.staging_address is not None
        assert summary.run_id[:8] in result.staging_address

    def test_the_result_holds_the_test_results(self, project: Path, fake_environment):
        summary = clair.run(project, adapter=_make_adapter())
        result = summary.result(DAILY_ORDERS)

        assert result is not None
        assert result.test_results
        assert all(test_result.passed for test_result in result.test_results)
        assert summary.test_results == result.test_results

    def test_a_failure_gives_the_error_and_the_sql(self, project: Path, fake_environment):
        summary = clair.run(
            project, adapter=_make_adapter(fail_on={"daily_orders"}), test=False
        )

        assert summary.failed_count == 1
        result = summary.failed[0]
        assert "Simulated failure" in result.error
        assert result.sql

    def test_a_selection_that_matches_nothing_gives_an_empty_summary(
        self, project: Path, fake_environment
    ):
        summary = clair.run(project, select=["no.such.trouve"], adapter=_make_adapter())

        assert summary.results == []
        assert summary.succeeded_count == 0

    def test_it_does_not_close_an_adapter_that_the_caller_gave(
        self, project: Path, fake_environment
    ):
        """A notebook keeps one connection open for many calls."""
        adapter = _make_adapter()
        clair.run(project, adapter=adapter, test=False)

        adapter.close.assert_not_called()


class TestTest:
    def test_it_gives_one_result_for_each_test(self, project: Path, fake_environment):
        summary = clair.test(project, adapter=_make_adapter())

        assert summary.results
        assert summary.failed_count == 0
        assert summary.error_count == 0

    def test_a_selection_that_matches_nothing_gives_an_empty_summary(
        self, project: Path, fake_environment
    ):
        summary = clair.test(project, select=["no.such.trouve"], adapter=_make_adapter())

        assert summary.results == []


class TestCatalog:
    def test_it_needs_no_connection(self, project: Path):
        project_catalog = clair.catalog(project)

        assert project_catalog["trouves"]
