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
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.staging import make_staging_address
from clair.trouves.run_config import IncrementalMode
from tests.integration.config import IntegrationConfig
from tests.integration.conftest import (
    clair_environment,
    events_named,
    run_clair,
    run_id_of,
)
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
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
        clair_home: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]]:
        """Build the table, then merge a change and a new row into it.

        The first run finds no physical table, so clair changes to the full
        refresh mode. The second run merges: it changes id_001 and it adds
        id_003.
        """
        schema_name = snowflake_workspace.schema_name
        environment = clair_environment(snowflake_workspace, clair_home)
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_upsert_pass"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        first = run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
        )

        make_source_rows(
            adapter,
            self.DATABASE_NAME,
            schema_name,
            {"id_000": 1, "id_001": 99, "id_002": 1, "id_003": 1},
        )
        second = run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
        )
        return first, second

    def test_the_first_run_changes_to_the_full_refresh_mode(
        self,
        runs: tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]],
    ) -> None:
        """No physical table exists yet, so clair makes no clone."""
        first, _ = runs
        fallbacks = events_named(first, "run.node.incremental_fallback")
        assert [event.get("logical") for event in fallbacks] == [
            f"{self.DATABASE_NAME}.refined.checked"
        ]

    def test_the_merge_updates_a_row_and_inserts_a_row(
        self,
        runs: tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The clone seeds the staging table, so the old rows stay."""
        schema_name = snowflake_workspace.schema_name
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert row_count(adapter, checked) == 4
        assert _amount_of(adapter, self.DATABASE_NAME, schema_name, "id_001") == 99
        assert _amount_of(adapter, self.DATABASE_NAME, schema_name, "id_000") == 1

    def test_clair_drops_the_staging_table_and_the_merge_source(
        self,
        runs: tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """An UPSERT makes two objects. A run that passed keeps neither."""
        _, second = runs
        schema_name = snowflake_workspace.schema_name
        run_id = run_id_of(second)
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(adapter, make_staging_address(checked, run_id))
        assert not table_exists(
            adapter, merge_source_address(checked, run_id, schema_name)
        )
        assert staging_objects(adapter, schema_name, self.DATABASE_NAME) == []


class TestAnUpsertRunWhoseTestFails:
    """The rejected candidate stays. The merge source table does not."""

    DATABASE_NAME = "staging_upsert_fail_database"

    @pytest.fixture(scope="class")
    def failed_run(
        self,
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
        clair_home: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> subprocess.CompletedProcess[str]:
        """Build 3 rows, then merge a 4th row that the test rejects."""
        schema_name = snowflake_workspace.schema_name
        environment = clair_environment(snowflake_workspace, clair_home)
        destination = tmp_path_factory.mktemp("staging_upsert_fail")

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        good_project = write_probe_project(
            destination / "good",
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )
        run_clair(
            ["run", "--project", str(good_project), "--run-mode", "incremental"],
            environment,
        )

        insert_source_row(adapter, self.DATABASE_NAME, schema_name, "id_003", 1)
        bad_project = write_probe_project(
            destination / "bad",
            self.DATABASE_NAME,
            minimum_rows=FAILING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )
        return run_clair(
            ["run", "--project", str(bad_project), "--run-mode", "incremental"],
            environment,
            expect_success=False,
        )

    def test_the_physical_table_keeps_the_rows_of_the_run_that_passed(
        self,
        failed_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE ran on the clone, so it never touched this table."""
        checked = checked_address(self.DATABASE_NAME, snowflake_workspace.schema_name)
        assert failed_run.returncode == 1
        assert row_count(adapter, checked) == 3

    def test_the_rejected_candidate_holds_the_merged_rows(
        self,
        failed_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """You can query the exact rows that the test rejected."""
        schema_name = snowflake_workspace.schema_name
        run_id = run_id_of(failed_run)
        checked = checked_address(self.DATABASE_NAME, schema_name)

        staging = make_staging_address(checked, run_id)
        assert table_exists(adapter, staging)
        assert row_count(adapter, staging) == 4

    def test_the_merge_source_table_goes_away(
        self,
        failed_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE passed, thus its own drop statement ran before the test."""
        schema_name = snowflake_workspace.schema_name
        run_id = run_id_of(failed_run)
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(
            adapter, merge_source_address(checked, run_id, schema_name)
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
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
        clair_home: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> subprocess.CompletedProcess[str]:
        """Give the SOURCE two rows with one id, thus the MERGE fails.

        Snowflake refuses a MERGE when two source rows match one target row.
        The message is "Duplicate row detected during DML action".
        """
        schema_name = snowflake_workspace.schema_name
        environment = clair_environment(snowflake_workspace, clair_home)
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_merge_error"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.UPSERT,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
        )

        insert_source_row(adapter, self.DATABASE_NAME, schema_name, "id_000", 42)
        return run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
            expect_success=False,
        )

    def test_the_merge_is_the_statement_that_failed(
        self, failed_run: subprocess.CompletedProcess[str]
    ) -> None:
        """The error must name the duplicate row, and no other fault.

        The next test asks if clair dropped the merge source table. That
        question means nothing when the run stopped before the MERGE, because
        clair makes the merge source table one statement earlier. This test
        therefore reads the message of Snowflake.
        """
        failures = events_named(failed_run, "run.node.failure")
        assert failed_run.returncode == 1
        assert [event.get("logical") for event in failures] == [
            f"{self.DATABASE_NAME}.refined.checked"
        ]
        assert "duplicate row" in str(failures[0].get("error")).lower()

    def test_clair_drops_the_merge_source_table(
        self,
        failed_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """This is the cleanup that the index from the end makes correct."""
        schema_name = snowflake_workspace.schema_name
        run_id = run_id_of(failed_run)
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(
            adapter, merge_source_address(checked, run_id, schema_name)
        )

    def test_the_physical_table_keeps_its_rows(
        self,
        failed_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        checked = checked_address(self.DATABASE_NAME, snowflake_workspace.schema_name)
        assert row_count(adapter, checked) == 3


class TestAnAppendRun:
    """An APPEND makes no merge source table, and the clone still seeds."""

    DATABASE_NAME = "staging_append_database"

    @pytest.fixture(scope="class")
    def second_run(
        self,
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
        clair_home: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> subprocess.CompletedProcess[str]:
        """Build 3 rows, then append the same 3 rows again."""
        schema_name = snowflake_workspace.schema_name
        environment = clair_environment(snowflake_workspace, clair_home)
        project_path = write_probe_project(
            tmp_path_factory.mktemp("staging_append"),
            self.DATABASE_NAME,
            minimum_rows=PASSING_LIMIT,
            incremental_mode=IncrementalMode.APPEND,
        )

        make_source_rows(adapter, self.DATABASE_NAME, schema_name, FIRST_ROWS)
        run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
        )
        return run_clair(
            ["run", "--project", str(project_path), "--run-mode", "incremental"],
            environment,
        )

    def test_the_clone_seeds_the_staging_table(
        self,
        second_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """3 rows of the first run, plus the 3 rows that the INSERT adds.

        Without the clone the staging table would start empty, and the
        promotion would then give the physical table 3 rows.
        """
        checked = checked_address(self.DATABASE_NAME, snowflake_workspace.schema_name)
        assert row_count(adapter, checked) == 6

    def test_clair_makes_no_merge_source_table(
        self,
        second_run: subprocess.CompletedProcess[str],
        snowflake_workspace: IntegrationConfig,
        adapter: SnowflakeAdapter,
    ) -> None:
        """The MERGE belongs to the UPSERT mode only."""
        schema_name = snowflake_workspace.schema_name
        run_id = run_id_of(second_run)
        checked = checked_address(self.DATABASE_NAME, schema_name)

        assert not table_exists(
            adapter, merge_source_address(checked, run_id, schema_name)
        )
        assert staging_objects(adapter, schema_name, self.DATABASE_NAME) == []
