"""Prove what staging does, against a real Snowflake account.

Clair writes each Trouve to a staging address, runs the data quality tests
there, and gives the data its physical address only after each test passes.
A mock adapter shows the statements that clair sends. It cannot show that
Snowflake accepts them, that a new object gets the future grant, or that the
physical table holds the rows of yesterday after a test failed. These tests do.

Each test calls `clair.run()` and reads the `RunSummary`. The summary names the
staging address of each Trouve, the status, and each data quality test result,
thus a test asks the run what happened.

Each test writes its own tables. The name of the project directory becomes the
first part of each logical address, thus two tests never collide in the schema
of the run.
"""

from __future__ import annotations

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunSummary
from clair.core.staging import STAGING_SUFFIX
from clair.environments.environments import Environment
from clair.trouves.trouve import ExecutionType
from tests.helpers import failure_of, success_of
from tests.integration.config import IntegrationConfig
from tests.integration.staging_project import (
    candidate_with_test_sql,
    checked_address,
    downstream_address,
    insert_source_row,
    make_source_rows,
    write_probe_project,
)
from tests.integration.warehouse import (
    privileges_on,
    query_rows,
    row_count,
    staging_objects,
    table_exists,
)

pytestmark = pytest.mark.integration

# TestRowCount(min_rows=PASSING_LIMIT) passes for every row count of these
# tests. TestRowCount(min_rows=FAILING_LIMIT) never passes.
PASSING_LIMIT = 1
FAILING_LIMIT = 1_000_000


def _make_source_rows(
    adapter: SnowflakeAdapter, database_name: str, schema_name: str, rows: int
) -> None:
    """Make the SOURCE table of one probe project, with *rows* rows."""
    make_source_rows(
        adapter,
        database_name,
        schema_name,
        {f"id_{index:03d}": 1 for index in range(rows)},
    )


def _staging_address_of(summary: RunSummary, logical_address: str) -> TrouveAddress:
    """Give the staging address that the run made for one Trouve.

    The run reports the address that it built at. A test therefore names the
    object of that run, and computes no name of its own.
    """
    result = summary.result(logical_address)
    assert result is not None, f"{logical_address} is not in the run"
    assert result.addresses.staging is not None, f"{logical_address} used no staging"
    return result.addresses.staging


class TestASuccessfulRun:
    """The physical address holds the tested data, and no candidate stays."""

    DATABASE_NAME = "staging_pass_database"

    @pytest.fixture(scope="class")
    def completed_run(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Run the probe project once, with a test that passes."""
        schema_name = clair_environment.schema_name
        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=3)
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_pass"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
        )
        return clair.run(project_path, env=environment)

    def test_the_physical_address_holds_the_rows(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The promotion moves the data to the address that the SQL names."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert table_exists(adapter, checked)
        assert row_count(adapter, checked) == 3

    def test_the_dependent_reads_the_promoted_data(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """Clair promotes each node before the next node starts."""
        downstream = downstream_address(
            self.DATABASE_NAME, clair_environment.schema_name
        )
        assert row_count(adapter, downstream) == 3

    def test_clair_drops_the_staging_object(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """A run that passed leaves no object beside the physical one."""
        staging = _staging_address_of(
            completed_run, f"{self.DATABASE_NAME}.refined.checked"
        )

        assert not table_exists(adapter, staging)
        assert (
            staging_objects(adapter, clair_environment.schema_name, self.DATABASE_NAME)
            == []
        )

    def test_the_run_reports_a_success_for_each_trouve(
        self, completed_run: RunSummary
    ) -> None:
        assert completed_run.failed == []
        assert completed_run.succeeded_count == 2

    def test_each_data_quality_test_passed(self, completed_run: RunSummary) -> None:
        """The result of each test comes back with the run that ran it."""
        assert completed_run.test_results
        assert [test_result.passed for test_result in completed_run.test_results] == [
            True for _ in completed_run.test_results
        ]


class TestAFailedDataQualityTest:
    """The promise of staging: bad data never reaches the physical address."""

    DATABASE_NAME = "staging_fail_database"

    @pytest.fixture(scope="class")
    def failed_run(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Build good data, then run again with a test that fails.

        The first run gives the physical table 3 rows. The second run finds 5
        rows in the SOURCE, and its test rejects them. The physical table must
        still hold the 3 rows of the first run.
        """
        schema_name = clair_environment.schema_name
        destination = tmp_path_factory.mktemp("staging_fail")

        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=3)
        good_project = write_probe_project(
            destination / "good", self.DATABASE_NAME, minimum_rows=PASSING_LIMIT
        )
        clair.run(good_project, env=environment)

        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=5)
        bad_project = write_probe_project(
            destination / "bad", self.DATABASE_NAME, minimum_rows=FAILING_LIMIT
        )
        return clair.run(bad_project, env=environment)

    def test_the_run_reports_the_failure_of_the_candidate(
        self, failed_run: RunSummary
    ) -> None:
        """The failed test makes the node a failure, and the run says so."""
        failure_of(failed_run, f"{self.DATABASE_NAME}.refined.checked")
        assert failed_run.failed_count == 1

    def test_the_run_names_the_test_that_rejected_the_data(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
    ) -> None:
        """The run says which test failed, and on which Trouve.

        This is the assertion that an exit code cannot make. The test ran
        against the staging object, and it reports the physical address, thus
        the reader sees the Trouve and not the candidate.
        """
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        result = failure_of(failed_run, f"{self.DATABASE_NAME}.refined.checked")
        assert [test.passed for test in result.test_results] == [False]
        assert result.test_results[0].test_type == "row_count"
        assert result.test_results[0].address == checked
        assert result.test_results[0].statement.query_id

    def test_the_physical_address_keeps_the_data_of_the_run_that_passed(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The 5 rejected rows never reach the physical table."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert row_count(adapter, checked) == 3

    def test_clair_keeps_the_rejected_candidate(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The candidate holds the exact data that failed the test."""
        staging = _staging_address_of(
            failed_run, f"{self.DATABASE_NAME}.refined.checked"
        )

        assert table_exists(adapter, staging)
        assert row_count(adapter, staging) == 5
        assert str(staging).endswith(f"{STAGING_SUFFIX}{failed_run.run_id}")
        # The error names the object, thus a person can query it.
        result = failure_of(failed_run, f"{self.DATABASE_NAME}.refined.checked")
        assert str(staging) in result.error

    def test_clair_skips_the_dependent(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """A dependent of a Trouve that failed never runs."""
        downstream = downstream_address(
            self.DATABASE_NAME, clair_environment.schema_name
        )
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)

        assert [str(result.addresses.logical) for result in failed_run.skipped] == [
            f"{self.DATABASE_NAME}.derived.downstream"
        ]
        assert failed_run.skipped[0].skipped_by == str(checked)
        # The first run built it, thus it exists. It holds the old data.
        assert row_count(adapter, downstream) == 3


class TestATestSqlReadsTheCandidate:
    """A TestSql on {THIS} examines the staging object, not production.

    The first run promotes rows that pass. Then the source gets a negative
    amount. If the test read production, it would read the clean rows of the
    first run, pass, and promote the negative amount.
    """

    DATABASE_NAME = "staging_test_sql_database"

    @pytest.fixture(scope="class")
    def second_run(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Run once with clean rows, add a negative amount, and run again."""
        schema_name = clair_environment.schema_name
        make_source_rows(adapter, self.DATABASE_NAME, schema_name, {"id_001": 5, "id_002": 7})
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_test_sql"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            checked_file=candidate_with_test_sql(self.DATABASE_NAME),
        )
        first_run = clair.run(project_path, env=environment)
        success_of(first_run, f"{self.DATABASE_NAME}.refined.checked")

        insert_source_row(adapter, self.DATABASE_NAME, schema_name, "id_003", -1)
        return clair.run(project_path, env=environment)

    def test_the_test_fails_on_the_candidate(self, second_run: RunSummary) -> None:
        failure = failure_of(second_run, f"{self.DATABASE_NAME}.refined.checked")

        (test_result,) = failure.test_results
        assert test_result.failing_row_count == 1
        assert failure.addresses.staging is not None
        assert str(failure.addresses.staging) in test_result.statement.sql

    def test_production_keeps_the_clean_rows(
        self,
        second_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)

        assert query_rows(adapter, f"select count(*) from {checked} where amount < 0") == [(0,)]
        assert row_count(adapter, checked) == 2


class TestAPandasTableThatPasses:
    """A pandas Trouve takes a different path to the staging address.

    A SQL Trouve runs ``CREATE OR REPLACE TABLE`` there. A pandas Trouve calls
    ``write_pandas``, and that function makes the table itself. Clair then
    promotes that table with the same clone. This test runs that combination.
    """

    DATABASE_NAME = "staging_pandas_pass_database"

    @pytest.fixture(scope="class")
    def completed_run(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        schema_name = clair_environment.schema_name
        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=3)
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_pandas_pass"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            execution_type=ExecutionType.PANDAS,
        )
        return clair.run(project_path, env=environment)

    def test_the_physical_address_holds_the_rows(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """Snowflake accepts a clone of the table that write_pandas made."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert table_exists(adapter, checked)
        assert row_count(adapter, checked) == 3

    def test_a_sql_dependent_reads_the_promoted_data(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The downstream Trouve is SQL, and it reads the pandas Trouve."""
        downstream = downstream_address(
            self.DATABASE_NAME, clair_environment.schema_name
        )
        assert row_count(adapter, downstream) == 3

    def test_clair_drops_the_staging_object(
        self,
        completed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        staging = _staging_address_of(
            completed_run, f"{self.DATABASE_NAME}.refined.checked"
        )

        assert not table_exists(adapter, staging)
        assert (
            staging_objects(adapter, clair_environment.schema_name, self.DATABASE_NAME)
            == []
        )


class TestAPandasTableThatFails:
    """The physical table keeps its rows when the test of a DataFrame fails."""

    DATABASE_NAME = "staging_pandas_fail_database"

    @pytest.fixture(scope="class")
    def failed_run(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Build 3 good rows, then write 5 rows that the test rejects."""
        schema_name = clair_environment.schema_name
        destination = tmp_path_factory.mktemp("staging_pandas_fail")

        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=3)
        good_project = write_probe_project(
            destination / "good",
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            execution_type=ExecutionType.PANDAS,
        )
        clair.run(good_project, env=environment)

        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=5)
        bad_project = write_probe_project(
            destination / "bad",
            self.DATABASE_NAME,
            minimum_rows=FAILING_LIMIT,
            execution_type=ExecutionType.PANDAS,
        )
        return clair.run(bad_project, env=environment)

    def test_the_run_reports_the_failure_of_the_candidate(
        self, failed_run: RunSummary
    ) -> None:
        result = failure_of(failed_run, f"{self.DATABASE_NAME}.refined.checked")
        assert [test.passed for test in result.test_results] == [False]

    def test_the_physical_address_keeps_the_data_of_the_run_that_passed(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        """write_pandas replaces a table, so it must never touch this one."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert row_count(adapter, checked) == 3

    def test_clair_keeps_the_rejected_candidate(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
    ) -> None:
        staging = _staging_address_of(
            failed_run, f"{self.DATABASE_NAME}.refined.checked"
        )

        assert table_exists(adapter, staging)
        assert row_count(adapter, staging) == 5


class TestEachPathKeepsTheFutureGrant:
    """Clair supports future grants on the schema, and no grant on one object (staging.md#grants)."""

    @pytest.mark.parametrize(
        ("use_staging", "database_name"),
        [(True, "staging_grants_database"), (False, "staging_grants_unstaged_database")],
        ids=["staged", "no_test"],
    )
    def test_production_holds_the_future_grant_after_each_run(
        self,
        use_staging: bool,
        database_name: str,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        """Each run replaces production with a new object, and Snowflake gives it the future grant.

        The setup script runs ``GRANT SELECT ON FUTURE TABLES IN DATABASE`` for
        this role. The second run replaces the object that the first run made:
        by the promotion of a staged run, or by the build of a run with
        ``test=False``.
        """
        schema_name = clair_environment.schema_name
        project_path = write_probe_project(
            tmp_path_factory.mktemp(database_name),
            database_name,
            minimum_rows=PASSING_LIMIT,
        )
        checked = checked_address(database_name, schema_name)

        _make_source_rows(adapter, database_name, schema_name, rows=3)
        clair.run(project_path, env=environment, test=use_staging)
        summary = clair.run(project_path, env=environment, test=use_staging)

        assert summary.failed == []
        assert "SELECT" in privileges_on(adapter, checked)


class TestTheTestFalseFlag:
    """Without the tests nothing decides a promotion, so clair writes direct."""

    DATABASE_NAME = "staging_notest_database"

    def test_the_run_writes_to_the_physical_address_and_makes_no_candidate(
        self,
        clair_environment: IntegrationConfig,
        environment: Environment,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        schema_name = clair_environment.schema_name
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_notest"),
            self.DATABASE_NAME,
            minimum_rows=FAILING_LIMIT,
        )
        checked = checked_address(self.DATABASE_NAME, schema_name)

        _make_source_rows(adapter, self.DATABASE_NAME, schema_name, rows=3)
        summary = clair.run(project_path, env=environment, test=False)

        result = success_of(summary, f"{self.DATABASE_NAME}.refined.checked")
        # The test of this project always fails. The run passes, thus clair did
        # not run it, and it did not decide the promotion.
        assert summary.failed == []
        assert result.test_results == []
        assert result.addresses.staging is None
        assert row_count(adapter, checked) == 3
        assert staging_objects(adapter, schema_name, self.DATABASE_NAME) == []
