"""Prove a sampled run against Snowflake: ``clair.run(sample=True)``.

The unit tests show the subquery in the SQL. Only the warehouse shows that the
subquery is valid SQL, that a row limit reads the number of rows, and that a
sample_sql that reads a seed carries its filter through a join.

The fixture writes two Source tables of four rows: ``events`` (ids 1 to 4) and
``users`` (ids 1 to 4, user ids u1 to u4). The seed ``test_users`` holds u2 and
u3. The run reads three rows of each Trouve without a sample_sql.
"""

from __future__ import annotations

import pytest

import clair
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunSummary
from clair.environments.environments import Environment
from clair.trouves.trouve import DEFAULT_SAMPLE_ROWS
from tests.integration.config import IntegrationConfig
from tests.integration.projects import physical_address, write_refined_project
from tests.integration.warehouse import execute, query_rows, row_count

pytestmark = pytest.mark.integration

DATABASE_NAME = "sample_rows"
SAMPLE_ROWS = 3
THE_TEST_USER_IDS = [2, 3]

SOURCE_ROWS = {
    "events": "SELECT column1 AS id FROM VALUES (1), (2), (3), (4)",
    "users": (
        "SELECT column1 AS id, column2 AS user_id "
        "FROM VALUES (1, 'u1'), (2, 'u2'), (3, 'u3'), (4, 'u4')"
    ),
}


def reader_file(upstream_name: str, sample_sql: str | None = None) -> str:
    """Give a Table that reads the id of one upstream Trouve."""
    sample_argument = f'    sample_sql=f"{sample_sql}",\n' if sample_sql else ""
    return (
        f"from {DATABASE_NAME}.refined.{upstream_name} import trouve as {upstream_name}\n"
        "from clair import THIS, Table\n"
        "trouve = Table(\n"
        f'    sql=f"SELECT id FROM {{{upstream_name}}}",\n'
        f"{sample_argument}"
        ")\n"
    )


USERS_FILE = f"""\
from {DATABASE_NAME}.refined.test_users import trouve as test_users

from clair import THIS, Source

trouve = Source(
    sample_sql=f"SELECT * FROM {{THIS}} WHERE user_id IN (SELECT user_id FROM {{test_users}})",
)
"""

TEST_USERS_FILE = """\
import pandas as pd

from clair import SeedTable

test_users = pd.DataFrame({"user_id": ["u2", "u3"]})
test_users["user_id"] = test_users["user_id"].astype("string")

trouve = SeedTable(dataframe=test_users)
"""

FRAME_IDS_FILE = f"""\
import pandas as pd

from {DATABASE_NAME}.refined.users import trouve as users
from clair import PandasTable


def ids_of(users: pd.DataFrame) -> pd.DataFrame:
    return users[["id"]]


trouve = PandasTable(transform=ids_of, inputs=[users])
"""

TROUVE_FILES = {
    "events": "from clair import Source\n\ntrouve = Source()\n",
    "users": USERS_FILE,
    "test_users": TEST_USERS_FILE,
    # A sample of limited_ids reads no row. The run builds limited_ids, thus a
    # reader of it reads the complete table, and not the sample.
    "limited_ids": reader_file("events", sample_sql="SELECT * FROM {THIS} WHERE 1 = 0"),
    "chosen_ids": reader_file("users"),
    "frame_ids": FRAME_IDS_FILE,
    "layer_ids": reader_file("limited_ids"),
}


class TestASampledRun:
    @pytest.fixture(scope="class")
    def summary(
        self,
        environment: Environment,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        for table_name, rows_sql in SOURCE_ROWS.items():
            address = physical_address(
                f"{DATABASE_NAME}.refined.{table_name}", clair_environment.schema_name
            )
            execute(adapter, f"CREATE OR REPLACE TABLE {address} AS {rows_sql}")
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DATABASE_NAME), DATABASE_NAME, TROUVE_FILES
        )
        return clair.run(project_path, env=environment, sample=True, sample_rows=SAMPLE_ROWS)

    def ids_of(
        self, adapter: SnowflakeAdapter, clair_environment: IntegrationConfig, table_name: str
    ) -> list[object]:
        address = physical_address(
            f"{DATABASE_NAME}.refined.{table_name}", clair_environment.schema_name
        )
        return [row[0] for row in query_rows(adapter, f"SELECT id FROM {address} ORDER BY id")]

    def test_each_trouve_succeeds(self, summary: RunSummary) -> None:
        assert summary.failed == [], [result.error for result in summary.failed]
        # Each file except the two Source files is a Trouve that the run builds.
        assert summary.succeeded_count == len(TROUVE_FILES) - len(SOURCE_ROWS)

    @pytest.mark.parametrize("table_name", ["limited_ids", "layer_ids"])
    def test_the_row_limit_reads_that_number_of_rows(
        self,
        summary: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        table_name: str,
    ) -> None:
        """layer_ids reads limited_ids of the run, thus it keeps each of the three rows."""
        assert summary.failed == []
        assert len(self.ids_of(adapter, clair_environment, table_name)) == SAMPLE_ROWS

    @pytest.mark.parametrize("table_name", ["chosen_ids", "frame_ids"])
    def test_a_sample_sql_carries_the_seed_through_the_read(
        self,
        summary: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        table_name: str,
    ) -> None:
        assert summary.failed == []
        assert self.ids_of(adapter, clair_environment, table_name) == THE_TEST_USER_IDS


DEFAULT_LIMIT_DATABASE_NAME = "sample_default"
# More rows than the default row limit, thus the limit is visible.
SOURCE_ROW_COUNT = DEFAULT_SAMPLE_ROWS + 500
# ROW_NUMBER gives the ids 1 to SOURCE_ROW_COUNT with no gap. SEQ4 alone can
# give a gap.
NUMBERED_ROWS_SQL = (
    "SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS id "
    f"FROM TABLE(GENERATOR(ROWCOUNT => {SOURCE_ROW_COUNT}))"
)
FILTERED_ROW_COUNT = 10


def source_file(sample_sql: str | None) -> str:
    """Give a Source, with a sample_sql or without one."""
    sample_argument = f'sample_sql=f"{sample_sql}"' if sample_sql else ""
    return f"from clair import THIS, Source\n\ntrouve = Source({sample_argument})\n"


def counted_reader_file(upstream_name: str) -> str:
    """Give a Table that reads the id of one Source, and tests that no id is NULL."""
    return (
        f"from {DEFAULT_LIMIT_DATABASE_NAME}.refined.{upstream_name} import trouve as {upstream_name}\n"
        "from clair import Table, TestNotNull\n"
        "trouve = Table(\n"
        f'    sql=f"SELECT id FROM {{{upstream_name}}}",\n'
        '    tests=[TestNotNull(column="id")],\n'
        ")\n"
    )


# Each Source reads the same rows. The reader of each one has the name of the
# Source and the suffix `_ids`.
DEFAULT_LIMIT_SOURCE_FILES = {
    "unfiltered": source_file(None),
    "filtered": source_file(f"SELECT * FROM {{THIS}} WHERE id <= {FILTERED_ROW_COUNT}"),
    "emptied": source_file("SELECT * FROM {THIS} WHERE 1 = 0"),
}


class TestTheDefaultRowLimit:
    """A run with sample=True and no sample_rows reads DEFAULT_SAMPLE_ROWS rows.

    A Source without a sample_sql gives the row limit. A Source with a
    sample_sql gives the rows of that query, and the limit does not apply.
    A sample_sql that gives no row makes an empty table, and a TestNotNull on
    an empty table passes: zero rows hold a NULL.
    """

    @pytest.fixture(scope="class")
    def summary(
        self,
        environment: Environment,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        for source_name in DEFAULT_LIMIT_SOURCE_FILES:
            address = physical_address(
                f"{DEFAULT_LIMIT_DATABASE_NAME}.refined.{source_name}",
                clair_environment.schema_name,
            )
            execute(adapter, f"CREATE OR REPLACE TABLE {address} AS {NUMBERED_ROWS_SQL}")
        trouve_files = {
            **DEFAULT_LIMIT_SOURCE_FILES,
            **{
                f"{source_name}_ids": counted_reader_file(source_name)
                for source_name in DEFAULT_LIMIT_SOURCE_FILES
            },
        }
        project_path = write_refined_project(
            tmp_path_factory.mktemp(DEFAULT_LIMIT_DATABASE_NAME),
            DEFAULT_LIMIT_DATABASE_NAME,
            trouve_files,
        )
        return clair.run(project_path, env=environment, sample=True)

    def test_each_reader_succeeds(self, summary: RunSummary) -> None:
        assert summary.failed == [], [result.error for result in summary.failed]
        assert summary.succeeded_count == len(DEFAULT_LIMIT_SOURCE_FILES)
        assert summary.sample_rows == DEFAULT_SAMPLE_ROWS

    @pytest.mark.parametrize(
        ("table_name", "expected_row_count"),
        [
            ("unfiltered_ids", DEFAULT_SAMPLE_ROWS),
            ("filtered_ids", FILTERED_ROW_COUNT),
            ("emptied_ids", 0),
        ],
    )
    def test_each_reader_holds_the_rows_of_its_sample(
        self,
        summary: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
        table_name: str,
        expected_row_count: int,
    ) -> None:
        assert summary.failed == []
        address = physical_address(
            f"{DEFAULT_LIMIT_DATABASE_NAME}.refined.{table_name}", clair_environment.schema_name
        )
        assert row_count(adapter, address) == expected_row_count

    def test_a_not_null_test_passes_on_an_empty_table(self, summary: RunSummary) -> None:
        (test_result,) = summary.result(
            f"{DEFAULT_LIMIT_DATABASE_NAME}.refined.emptied_ids"
        ).test_results
        assert test_result.test_type == "not_null"
        assert test_result.passed
