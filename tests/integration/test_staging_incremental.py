"""Prove the staging steps of an incremental run, against Snowflake.

An incremental run adds two objects that a full refresh does not have:

* The **staging table**, ``<table>__clair_<run_id>``. A zero copy clone of the
  physical table seeds it, because an incremental statement changes data that
  exists already.
* The **merge source table**, ``<table>__clair_merge_<run_id>``. An UPSERT
  writes the new rows there, and the MERGE reads them. An APPEND makes no such
  table.

The order of the statements therefore is: clone, make the merge source, MERGE,
drop the merge source, test, promote, drop the staging table. The tests below
run each of those paths, and each one that fails.

Each test calls `clair.run()`. The `RunSummary` gives the statements of the run
and the index of the statement that failed, thus a test names the step that
Snowflake refused.
"""

from __future__ import annotations

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from clair.core.staging import make_staging_address
from clair.trouves.run_config import IncrementalMode, RunMode
from tests.integration.config import IntegrationConfig
from tests.integration.staging_project import (
    checked_address,
    insert_source_row,
    make_source_rows,
    merge_source_address,
    write_probe_project,
)
from tests.integration.warehouse import (
    query_rows,
    row_count,
    staging_objects,
    table_exists,
)

pytestmark = pytest.mark.integration

PASSING_LIMIT = 1
FAILING_LIMIT = 1_000_000

# The rows of the first run. The second run changes them.
FIRST_ROWS = {"id_000": 1, "id_001": 1, "id_002": 1}


def _amount_of(
    adapter: SnowflakeAdapter, database_name: str, schema_name: str, row_id: str
) -> int:
    """Give the amount of one row of the candidate table."""
    address = checked_address(database_name, schema_name)
    rows = query_rows(adapter, f"select amount from {address} where id = '{row_id}'")
    assert len(rows) == 1, f"{row_id} gives {len(rows)} rows, and 1 is correct"
    return int(str(rows[0][0]))


class TestAnUpsertRunThatPasses:
    """The MERGE writes to the staging table, and the promotion follows."""

    DATABASE_NAME = "staging_upsert_pass_database"

    @pytest.fixture(scope="class")
    def runs(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> tuple[RunSummary, RunSummary]:
        """Build the table, then merge a change and a new row into it.

        The first run finds no physical table, so clair changes to the full
        refresh mode. The second run merges: it changes id_001 and it adds
        id_003.
        """
        schema_name = clair_environment.schema_name
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_upsert_pass"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        first = clair.run(project_path, run_mode=RunMode.INCREMENTAL)

        make_source_rows(
            adapter,
            self.DATABASE_NAME,
            schema_name,
            {"id_000": 1, "id_001": 99, "id_002": 1, "id_003": 1},
        )
        second = clair.run(project_path, run_mode=RunMode.INCREMENTAL)
        return first, second

    def test_the_first_run_changes_to_the_full_refresh_mode(
        self, runs: tuple[RunSummary, RunSummary]
    ) -> None:
        """No physical table exists yet, so clair makes no clone."""
        first, _ = runs
        result = first.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert result.effective_run_mode == RunMode.FULL_REFRESH

    def test_the_second_run_stays_incremental(
        self, runs: tuple[RunSummary, RunSummary]
    ) -> None:
        """The table exists now, thus the incremental statements run."""
        _, second = runs
        result = second.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert result.effective_run_mode == RunMode.INCREMENTAL
        # The clone seeds the staging table, and the MERGE follows it.
        assert "clone" in result.statements[0].sql.lower()
        assert any("merge into" in s.sql.lower() for s in result.statements)

    def test_the_merge_updates_a_row_and_inserts_a_row(
        self,
        runs: tuple[RunSummary, RunSummary],
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The clone seeds the staging table, so the old rows stay."""
        schema_name = clair_environment.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert row_count(adapter, checked) == 4
        assert _amount_of(adapter, self.DATABASE_NAME, schema_name, "id_001") == 99
        assert _amount_of(adapter, self.DATABASE_NAME, schema_name, "id_000") == 1

    def test_clair_drops_the_staging_table_and_the_merge_source(
        self,
        runs: tuple[RunSummary, RunSummary],
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """An UPSERT makes two objects. A run that passed keeps neither."""
        _, second = runs
        schema_name = clair_environment.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(adapter, make_staging_address(checked, second.run_id))
        assert not table_exists(
            adapter, merge_source_address(checked, second.run_id, schema_name)
        )
        assert staging_objects(adapter, schema_name, self.DATABASE_NAME) == []


class TestAnUpsertRunWhoseTestFails:
    """The rejected candidate stays. The merge source table does not."""

    DATABASE_NAME = "staging_upsert_fail_database"

    @pytest.fixture(scope="class")
    def failed_run(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Build 3 rows, then merge a 4th row that the test rejects."""
        schema_name = clair_environment.schema_name
        destination = tmp_path_factory.mktemp("staging_upsert_fail")

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        good_project = write_probe_project(
            destination / "good",
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )
        clair.run(good_project, run_mode=RunMode.INCREMENTAL)

        insert_source_row(adapter, self.DATABASE_NAME, schema_name, "id_003", 1)
        bad_project = write_probe_project(
            destination / "bad",
            self.DATABASE_NAME,
            minimum_rows=FAILING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )
        return clair.run(bad_project, run_mode=RunMode.INCREMENTAL)

    def test_the_physical_table_keeps_the_rows_of_the_run_that_passed(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE ran on the clone, so it never touched this table."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        result = failed_run.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert result.status == RunStatus.FAILURE
        assert [test.passed for test in result.test_results] == [False]
        assert row_count(adapter, checked) == 3

    def test_the_rejected_candidate_holds_the_merged_rows(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """You can query the exact rows that the test rejected."""
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        staging = make_staging_address(checked, failed_run.run_id)

        assert table_exists(adapter, staging)
        assert row_count(adapter, staging) == 4

    def test_the_merge_source_table_goes_away(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE passed, thus its own drop statement ran before the test."""
        schema_name = clair_environment.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(
            adapter, merge_source_address(checked, failed_run.run_id, schema_name)
        )


class TestAMergeThatFails:
    """The cleanup drops the merge source table after the MERGE fails.

    The runner counts the statements from the end of the list. A staged
    incremental run puts a clone in front, so an index from the front names the
    wrong statement, and the merge source table would stay behind for ever.
    """

    DATABASE_NAME = "staging_merge_error_database"

    @pytest.fixture(scope="class")
    def failed_run(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Give the SOURCE two rows with one id, thus the MERGE fails.

        Snowflake refuses a MERGE when two source rows match one target row.
        The message is "Duplicate row detected during DML action".
        """
        schema_name = clair_environment.schema_name
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_merge_error"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        clair.run(project_path, run_mode=RunMode.INCREMENTAL)

        insert_source_row(adapter, self.DATABASE_NAME, schema_name, "id_000", 42)
        return clair.run(project_path, run_mode=RunMode.INCREMENTAL)

    def test_the_merge_is_the_statement_that_failed(
        self, failed_run: RunSummary
    ) -> None:
        """The MERGE must be the statement that stopped, and no other one.

        The next test asks if clair dropped the merge source table. That
        question means nothing when the run stopped before the MERGE, because
        clair makes the merge source table one statement earlier. The result
        names the statement, thus this test reads the SQL of that step.
        """
        result = failed_run.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert result.status == RunStatus.FAILURE
        assert failed_run.failed_count == 1
        assert result.failed_statement is not None
        assert "merge into" in result.failed_statement.sql.lower()
        assert "duplicate row" in result.error.lower()

    def test_the_run_ran_no_data_quality_test(self, failed_run: RunSummary) -> None:
        """A build that fails never reaches the test step."""
        result = failed_run.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert result.test_results == []

    def test_clair_drops_the_merge_source_table(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """This is the cleanup that the index from the end makes correct."""
        schema_name = clair_environment.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(
            adapter, merge_source_address(checked, failed_run.run_id, schema_name)
        )

    def test_the_physical_table_keeps_its_rows(
        self,
        failed_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert row_count(adapter, checked) == 3


class TestAnAppendRun:
    """An APPEND makes no merge source table, and the clone still seeds."""

    DATABASE_NAME = "staging_append_database"

    @pytest.fixture(scope="class")
    def second_run(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Build 3 rows, then append the same 3 rows again."""
        schema_name = clair_environment.schema_name
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_append"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.APPEND,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        clair.run(project_path, run_mode=RunMode.INCREMENTAL)
        return clair.run(project_path, run_mode=RunMode.INCREMENTAL)

    def test_the_clone_seeds_the_staging_table(
        self,
        second_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """3 rows of the first run, plus the 3 rows that the INSERT adds.

        Without the clone the staging table would start empty, and the
        promotion would then give the physical table 3 rows.
        """
        checked = checked_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert row_count(adapter, checked) == 6

    def test_clair_makes_no_merge_source_table(
        self,
        second_run: RunSummary,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE belongs to the UPSERT mode only."""
        schema_name = clair_environment.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)
        result = second_run.result(f"{self.DATABASE_NAME}.refined.checked")

        assert result is not None
        assert not any("merge into" in s.sql.lower() for s in result.statements)
        assert not table_exists(
            adapter, merge_source_address(checked, second_run.run_id, schema_name)
        )
        assert staging_objects(adapter, schema_name, self.DATABASE_NAME) == []
